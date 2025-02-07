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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.TopicInfo;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Topology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer.ExpiredTimeout;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer.ScheduledTimeout;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue.Endpoint;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.coordinator.group.streams.CoordinatorStreamsRecordHelpers;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.coordinator.group.streams.StreamsGroupBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.common.protocol.Errors.NOT_COORDINATOR;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedListEquals;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.EMPTY_RESULT;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.appendGroupMetadataErrorToResponseError;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.classicGroupHeartbeatKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.classicGroupJoinKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.classicGroupSyncKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupJoinKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupRebalanceTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupMetadataManagerTest {
    @Test
    public void testConsumerHeartbeatRequestValidation() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();
        Exception ex;

        // MemberId must be present in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()));
        assertEquals("MemberId can't be empty.", ex.getMessage());

        // GroupId must be present in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // GroupId can't be all whitespaces.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId("   ")));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // RebalanceTimeoutMs must be present in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId("foo")
                .setMemberEpoch(0)));
        assertEquals("RebalanceTimeoutMs must be provided in first request.", ex.getMessage());

        // TopicPartitions must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)));
        assertEquals("TopicPartitions must be empty when (re-)joining.", ex.getMessage());

        // SubscribedTopicNames or SubscribedTopicRegex must be present in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setTopicPartitions(Collections.emptyList())));
        assertEquals("Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when (re-)joining.", ex.getMessage());

        // InstanceId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setInstanceId("")));
        assertEquals("InstanceId can't be empty.", ex.getMessage());

        // RackId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setRackId("")));
        assertEquals("RackId can't be empty.", ex.getMessage());

        // ServerAssignor must exist if provided in all requests.
        ex = assertThrows(UnsupportedAssignorException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setServerAssignor("bar")));
        assertEquals("ServerAssignor bar is not supported. Supported assignors: range.", ex.getMessage());

        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));

        assertEquals("InstanceId can't be null.", ex.getMessage());
    }


    @Test
    public void testStreamsRequestValidation() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        Exception ex;
        String memberId = Uuid.randomUuid().toString();

        // GroupId must be present in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // GroupId can't be all whitespaces.
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("   ")));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // RebalanceTimeoutMs must be present in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)));
        assertEquals("RebalanceTimeoutMs must be provided in first request.", ex.getMessage());

        // ActiveTasks must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setMemberId(memberId)
                .setRebalanceTimeoutMs(5000)
                .setStandbyTasks(Collections.emptyList())
                .setWarmupTasks(Collections.emptyList())));
        assertEquals("ActiveTasks must be empty when (re-)joining.", ex.getMessage());

        // StandbyTasks must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setMemberId(memberId)
                .setRebalanceTimeoutMs(5000)
                .setActiveTasks(Collections.emptyList())
                .setWarmupTasks(Collections.emptyList())));
        assertEquals("StandbyTasks must be empty when (re-)joining.", ex.getMessage());

        // WarmupTasks must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setMemberId(memberId)
                .setRebalanceTimeoutMs(5000)
                .setActiveTasks(Collections.emptyList())
                .setStandbyTasks(Collections.emptyList())));
        assertEquals("WarmupTasks must be empty when (re-)joining.", ex.getMessage());

        // MemberId must be non-empty in all requests
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)));
        assertEquals("MemberId can't be null or empty.", ex.getMessage());

        // MemberId must not be null in all requests
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId("foo")
                        .setMemberId(null)
                        .setMemberEpoch(0)));
        assertEquals("MemberId can't be null or empty.", ex.getMessage());

        // InstanceId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setInstanceId("")));
        assertEquals("InstanceId can't be empty.", ex.getMessage());

        // RackId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setRackId("")));
        assertEquals("RackId can't be empty.", ex.getMessage());

        // InstanceId can't be null with static membership
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId("foo")
                        .setMemberId(memberId)
                        .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                        .setInstanceId(null)
                        .setRackId("rackid")));
        assertEquals("InstanceId can't be null.", ex.getMessage());

        // valid memberEpoch values are 0+, -1 (member leave group), or -2 (static member leave group)
        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId("foo")
                        .setMemberId(memberId)
                        .setMemberEpoch(-3)
                        .setInstanceId("bar")
                        .setRackId("baz")));
        assertEquals("MemberEpoch is invalid.", ex.getMessage());

        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setInstanceId("bar")
                .setRackId("baz")));
        assertEquals("RebalanceTimeoutMs must be provided in first request.", ex.getMessage());

        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setTopology(new Topology())));
        assertEquals("Topology can only be provided when (re-)joining.", ex.getMessage());

        ex = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1000)
                .setActiveTasks(Collections.emptyList())
                .setStandbyTasks(Collections.emptyList())
                .setWarmupTasks(Collections.emptyList())
                .setTopology(null)));
        assertEquals("Topology must be provided in first request.", ex.getMessage());
    }

    @Test
    public void testConsumerHeartbeatRegexValidation() {
        String memberId = Uuid.randomUuid().toString();
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Collections.emptyMap()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        // Subscribing with an invalid regular expression fails.
        Exception ex = assertThrows(InvalidRegularExpression.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(Uuid.randomUuid().toString())
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("[")
                .setTopicPartitions(Collections.emptyList())));
        assertEquals("SubscribedTopicRegex `[` is not a valid regular expression: missing closing ].", ex.getMessage());

        // Subscribing with a valid regular expression succeeds.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex(".*")
                .setTopicPartitions(Collections.emptyList()));
        assertEquals(1, result.response().memberEpoch());

        // Updating the subscription to an invalid regular expression fails.
        assertThrows(InvalidRegularExpression.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("[")
                .setTopicPartitions(Collections.emptyList())));
        assertEquals("SubscribedTopicRegex `[` is not a valid regular expression: missing closing ].", ex.getMessage());

        // Updating the subscription to topic names succeeds (checking when the regex becomes null).
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(Collections.emptyList()));
        assertEquals(2, result.response().memberEpoch());
    }

    @Test
    public void testJoiningNonExistingStreamsGroupNoMissingTopics() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int rebalanceTimeoutMs = 300000;
        int topologyEpoch = 0;
        String processId = "process-id";
        String subtopologyId = "subtopology-id";
        MockTaskAssignor assignor = new MockTaskAssignor("mock");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(Uuid.randomUuid(), "input-topic", 3)
                .addRacks()
                .build())
            .withTaskAssignors(Collections.singletonList(assignor))
            .build();
        final StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology();
        topology.setEpoch(topologyEpoch);
        topology.subtopologies().add(new StreamsGroupHeartbeatRequestData.Subtopology()
            .setSubtopologyId(subtopologyId)
            .setRepartitionSinkTopics(Collections.emptyList())
            .setRepartitionSourceTopics(Collections.emptyList())
            .setSourceTopics(Collections.singletonList("input-topic"))
            .setStateChangelogTopics(Collections.emptyList())
        );
        StreamsGroupHeartbeatRequestData heartbeat =
            buildFirstStreamsGroupHeartbeatRequest(groupId, topology, processId, rebalanceTimeoutMs, memberId);
        prepareStreamsGroupAssignment(assignor, heartbeat.memberId(), "subtopology-id");

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(heartbeat);

        assertNotNull(result.response());
        StreamsGroupHeartbeatResponseData response = result.response().responseData();
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertFalse(response.memberId().isEmpty());
        assertEquals(1, response.memberEpoch());
        assertFalse(response.activeTasks().isEmpty());
        assertTrue(response.standbyTasks().isEmpty());
        assertTrue(response.warmupTasks().isEmpty());
        List<CoordinatorRecord> coordinatorRecords = result.records();
        assertEquals(7, coordinatorRecords.size());
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord(groupId, 1)));
        StreamsGroupMember member = new StreamsGroupMember.Builder(response.memberId())
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setRebalanceTimeoutMs(rebalanceTimeoutMs)
            .setTopologyEpoch(topologyEpoch)
            .setProcessId(processId)
            .build();
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord(groupId, member)));
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId, 1)));
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                groupId,
                member.memberId(),
                Map.of(subtopologyId, new HashSet<>(List.of(0, 1, 2))),
                Collections.emptyMap(),
                Collections.emptyMap()
        )));
        assertTrue(coordinatorRecords.contains(
            CoordinatorStreamsRecordHelpers.newStreamsGroupTopologyRecord(
                groupId,
                topology
            )
        ));

        StreamsGroupHeartbeatRequestData.TaskIds ownedActiveTasks = new StreamsGroupHeartbeatRequestData.TaskIds();
        ownedActiveTasks.setSubtopologyId(subtopologyId);
        ownedActiveTasks.setPartitions(List.of(0, 1, 2));
        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(
                1,
                new org.apache.kafka.coordinator.group.streams.Assignment(Map.of(subtopologyId, Set.of(0, 1, 2)), Collections.emptyMap(), Collections.emptyMap())
            )
            .withOwnedActiveTasks(List.of(ownedActiveTasks))
            .withOwnedStandbyTasks(Collections.emptyList())
            .withOwnedWarmupTasks(Collections.emptyList())
            .withCurrentActiveTaskProcessId((s, p) -> null)
            .withCurrentStandbyTaskProcessIds((s, p) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((s, p) -> Collections.emptySet())
            .build();
        assertTrue(coordinatorRecords.contains(
            CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, updatedMember)
        ));
        assertEquals(StreamsGroup.StreamsGroupState.STABLE, context.streamsGroupState("group-id"));

        final Map<String, CreatableTopic> creatableTopic = result.response().creatableTopics();
        assertEquals(Collections.emptyMap(), creatableTopic);
    }

    @Test
    public void testJoiningNonExistingStreamsGroupMissingTopics() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int rebalanceTimeoutMs = 300000;
        int topologyEpoch = 0;
        String processId = "process-id";
        String subtopologyId = "subtopology-id";
        MockTaskAssignor assignor = new MockTaskAssignor("mock");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(Uuid.randomUuid(), "input-topic", 3)
                .addRacks()
                .build())
            .withTaskAssignors(Collections.singletonList(assignor))
            .build();
        final StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology();
        topology.setEpoch(topologyEpoch);
        topology.subtopologies().add(new StreamsGroupHeartbeatRequestData.Subtopology()
            .setSubtopologyId(subtopologyId)
            .setRepartitionSinkTopics(Collections.emptyList())
            .setRepartitionSourceTopics(Collections.emptyList())
            .setSourceTopics(Collections.singletonList("input-topic"))
            .setStateChangelogTopics(Collections.singletonList(new TopicInfo().setName("changelog-topic")))
        );
        StreamsGroupHeartbeatRequestData heartbeat =
            buildFirstStreamsGroupHeartbeatRequest(groupId, topology, processId, rebalanceTimeoutMs, memberId);
        prepareStreamsGroupAssignment(assignor, heartbeat.memberId(), "subtopology-id");

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(heartbeat);

        assertNotNull(result.response());
        StreamsGroupHeartbeatResponseData response = result.response().responseData();
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertFalse(response.memberId().isEmpty());
        assertEquals(1, response.memberEpoch());
        assertTrue(response.activeTasks().isEmpty());
        assertTrue(response.standbyTasks().isEmpty());
        assertTrue(response.warmupTasks().isEmpty());
        List<CoordinatorRecord> coordinatorRecords = result.records();
        assertEquals(7, coordinatorRecords.size());
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord(groupId, 1)));
        StreamsGroupMember member = new StreamsGroupMember.Builder(response.memberId())
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setRebalanceTimeoutMs(rebalanceTimeoutMs)
            .setTopologyEpoch(topologyEpoch)
            .setProcessId(processId)
            .build();
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord(groupId, member)));
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId, 1)));
        assertTrue(coordinatorRecords.contains(
            CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                groupId,
                member.memberId(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        ));
        assertTrue(coordinatorRecords.contains(
            CoordinatorStreamsRecordHelpers.newStreamsGroupTopologyRecord(
                groupId,
                topology
            )
        ));
        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(
                1,
                new org.apache.kafka.coordinator.group.streams.Assignment(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())
            )
            .withOwnedActiveTasks(Collections.emptyList())
            .withOwnedStandbyTasks(Collections.emptyList())
            .withOwnedWarmupTasks(Collections.emptyList())
            .build();
        assertTrue(coordinatorRecords.contains(
            CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, updatedMember)
        ));
        assertEquals(StreamsGroupState.NOT_READY, context.streamsGroupState("group-id"));

        final Map<String, CreatableTopic> creatableTopic = result.response().creatableTopics();
        assertEquals(
            Map.of("changelog-topic", new CreatableTopic().setName("changelog-topic").setNumPartitions(3).setReplicationFactor((short) -1)),
            creatableTopic
        );
    }

    @Test
    public void testJoiningExistingNotReadyStreamsGroupMissingTopics() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int rebalanceTimeoutMs = 300000;
        int topologyEpoch = 0;
        String processId = "process-id";
        MockTaskAssignor assignor = new MockTaskAssignor("mock");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(Uuid.randomUuid(), "bar", 3)
                .addRacks()
                .build())
            .withTaskAssignors(Collections.singletonList(assignor))
            .build();
        final StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology();
        topology.setEpoch(topologyEpoch);
        topology.subtopologies().add(
            new StreamsGroupHeartbeatRequestData.Subtopology()
                .setSubtopologyId("subtopology-id")
                .setSourceTopics(Collections.singletonList("bar"))
                .setRepartitionSourceTopics(
                    Collections.singletonList(
                        new StreamsGroupHeartbeatRequestData.TopicInfo()
                            .setName("repartition")
                            .setPartitions(4)
                            .setTopicConfigs(Collections.singletonList(
                                new StreamsGroupHeartbeatRequestData.KeyValue()
                                    .setKey("config-name1")
                                    .setValue("config-value1")
                            ))
                    )
                )
                .setStateChangelogTopics(
                    Collections.singletonList(
                        new StreamsGroupHeartbeatRequestData.TopicInfo()
                            .setName("changelog")
                            .setReplicationFactor((short) 1)
                            .setTopicConfigs(Collections.singletonList(
                                new StreamsGroupHeartbeatRequestData.KeyValue()
                                    .setKey("config-name2")
                                    .setValue("config-value2")
                            ))
                    )
                )
        );
        StreamsGroupHeartbeatRequestData heartbeatToCreateGroup =
            buildFirstStreamsGroupHeartbeatRequest(groupId, topology, processId, rebalanceTimeoutMs, memberId);
        prepareStreamsGroupAssignment(assignor, heartbeatToCreateGroup.memberId(), "subtopology-id");

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(heartbeatToCreateGroup);

        assertNotNull(result.response());
        Map<String, CreatableTopic> creatableTopics = result.response().creatableTopics();
        CreatableTopicConfigCollection expectedConfig1 = new CreatableTopicConfigCollection();
        expectedConfig1.add(
            new CreatableTopicConfig()
                .setName("config-name1")
                .setValue("config-value1")
        );
        CreatableTopicConfigCollection expectedConfig2 = new CreatableTopicConfigCollection();
        expectedConfig2.add(
            new CreatableTopicConfig()
                .setName("config-name2")
                .setValue("config-value2")
        );
        CreatableTopic expected1 =
            new CreatableTopic()
                .setName("repartition")
                .setNumPartitions(4)
                .setReplicationFactor((short) -1) // default
                .setConfigs(expectedConfig1);
        CreatableTopic expected2 =
            new CreatableTopic()
                .setName("changelog")
                .setNumPartitions(3)
                .setReplicationFactor((short) 1)
                .setConfigs(expectedConfig2);

        assertEquals(
            Map.of(
                "repartition", expected1,
                "changelog", expected2
            ),
            creatableTopics
        );

        StreamsGroupHeartbeatResponseData response = result.response().responseData();
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertFalse(response.memberId().isEmpty());
        assertEquals(1, response.memberEpoch());
        assertTrue(response.activeTasks().isEmpty());
        assertTrue(response.standbyTasks().isEmpty());
        assertTrue(response.warmupTasks().isEmpty());
        List<CoordinatorRecord> coordinatorRecords = result.records();
        assertEquals(7, coordinatorRecords.size());
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord(groupId, 1)));
        StreamsGroupMember member = new StreamsGroupMember.Builder(response.memberId())
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setRebalanceTimeoutMs(rebalanceTimeoutMs)
            .setTopologyEpoch(topologyEpoch)
            .setProcessId(processId)
            .build();
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord(groupId, member)));
        assertTrue(coordinatorRecords.contains(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId, 1)));
        assertTrue(coordinatorRecords.contains(
            CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                groupId,
                member.memberId(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        ));
        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(
                1,
                new org.apache.kafka.coordinator.group.streams.Assignment(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())
            )
            .withOwnedActiveTasks(Collections.emptyList())
            .withOwnedStandbyTasks(Collections.emptyList())
            .withOwnedWarmupTasks(Collections.emptyList())
            .build();
        assertTrue(coordinatorRecords.contains(
            CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, updatedMember)
        ));
        assertEquals(StreamsGroup.StreamsGroupState.NOT_READY, context.streamsGroupState("group-id"));
    }

    private StreamsGroupHeartbeatRequestData buildFirstStreamsGroupHeartbeatRequest(
            final String groupId,
            final StreamsGroupHeartbeatRequestData.Topology topology,
            final String processId,
            final int rebalanceTimeoutMs,
            final String memberId) {

        return new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setInstanceId(null)
            .setRackId(null)
            .setRebalanceTimeoutMs(rebalanceTimeoutMs)
            .setTopology(topology)
            .setActiveTasks(Collections.emptyList())
            .setStandbyTasks(Collections.emptyList())
            .setWarmupTasks(Collections.emptyList())
            .setProcessId(processId)
            .setUserEndpoint(null)
            .setClientTags(null)
            .setTaskOffsets(null)
            .setTaskEndOffsets(null)
            .setShutdownApplication(false);
    }

    private void prepareStreamsGroupAssignment(final MockTaskAssignor assignor,
                                               final String memberId,
                                               final String subtopologyId) {
        assignor.prepareGroupAssignment(new org.apache.kafka.coordinator.group.taskassignor.GroupAssignment(
            mkMap(
                mkEntry(
                    memberId,
                    new org.apache.kafka.coordinator.group.taskassignor.MemberAssignment(
                        mkMap(
                            mkEntry(
                                subtopologyId,
                                new HashSet<>(List.of(0, 1, 2))
                            )
                        ),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    )
                )
            )
        ));
    }

    @Test
    public void testMemberIdGeneration() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(MetadataImage.EMPTY)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.emptyMap()
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            // The consumer generates its own Member ID starting from version 1 of the ConsumerGroupHeartbeat RPC.
            // Therefore, this test case is specific to earlier versions of the RPC.
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("group-foo")
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()),
            (short) 0
        );

        // Verify that a member id was generated for the new member.
        String memberId = result.response().memberId();
        assertNotNull(memberId);
        assertNotEquals("", memberId);

        // The response should get a bumped epoch and should not
        // contain any assignment because we did not provide
        // topics metadata.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );
    }

    @Test
    public void testUnknownGroupId() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(100) // Epoch must be > 0.
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testUnknownMemberIdJoinsConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .build();

        // A first member joins to create the group.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // The second member is rejected because the member id is unknown and
        // the member epoch is not zero.
        assertThrows(UnknownMemberIdException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupMemberEpochValidation() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3)
        )));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 100));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        // Member epoch is greater than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(200)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member epoch is smaller than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(50)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch but without providing partitions.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(99)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch and has a subset of the owned partitions. This
        // is accepted as the response with the bumped epoch may have been lost. In this
        // case, we provide back the correct epoch to the member.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(99)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(1, 2)))));
        assertEquals(100, result.response().memberEpoch());
    }

    @Test
    public void testMemberJoinsEmptyConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.consumerGroup(groupId));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testUpdatingSubscriptionTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(List.of("foo", "bar")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testNewJoiningMemberTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            ))
        )));

        // Member 3 joins the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords.subList(0, 2), result.records().subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 5), result.records().subList(2, 5));
        assertRecordsEquals(expectedRecords.subList(5, 7), result.records().subList(5, 7));
    }

    @Test
    public void testLeavingMemberBumpsGroupEpoch() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with two members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH),
            result.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Subscription metadata is recomputed because zar is no longer there.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testGroupEpochBumpWhenNewStaticMemberJoins() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            ))
        )));

        // Member 3 joins the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setInstanceId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(11)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setInstanceId(memberId3)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords.subList(0, 2), result.records().subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 5), result.records().subList(2, 5));
        assertRecordsEquals(expectedRecords.subList(5, 7), result.records().subList(5, 7));
    }

    @Test
    public void testStaticMemberGetsBackAssignmentUponRejoin() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String member2RejoinId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId1)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId2)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
            .build();

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withSubscriptionMetadata(Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
                )))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId2)
                .setMemberEpoch(-2)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // Member epoch of the response would be set to -2.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(-2),
            result.response()
        );

        // The departing static member will have it's epoch set to -2.
        ConsumerGroupMember member2UpdatedEpoch = new ConsumerGroupMember.Builder(member2)
            .setMemberEpoch(-2)
            .build();

        assertEquals(1, result.records().size());
        assertRecordEquals(result.records().get(0), GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member2UpdatedEpoch));

        // Member 2 rejoins the group with the same instance id.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> rejoinResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(member2RejoinId)
                .setGroupId(groupId)
                .setInstanceId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member2RejoinId)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(2))
                    ))),
            rejoinResult.response()
        );

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setInstanceId(memberId2)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        ConsumerGroupMember expectedRejoinedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setInstanceId(memberId2)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        List<CoordinatorRecord> expectedRecordsAfterRejoin = List.of(
            // The previous member is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),

            // The previous member is replaced by the new one.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, member2RejoinId, mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember),

            // The new member is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedRejoinedMember)
        );

        assertRecordsEquals(expectedRecordsAfterRejoin, rejoinResult.records());
        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId2);
        context.assertNoRebalanceTimeout(groupId, memberId2);
    }

    @Test
    public void testStaticMemberRejoinsWithNewSubscribedTopics() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String member2RejoinId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setInstanceId("instance-id-1")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId("instance-id-2")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)))
            .build();

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withSubscriptionMetadata(
                    Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6))
                ))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)
            )),
            member2RejoinId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            ))
        )));

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId("instance-id-2")
                .setMemberEpoch(-2));

        // Member epoch of the response would be set to -2.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(-2),
            result.response()
        );

        // The departing static member will have it's epoch set to -2.
        ConsumerGroupMember member2UpdatedEpoch = new ConsumerGroupMember.Builder(member2)
            .setMemberEpoch(-2)
            .build();

        assertEquals(1, result.records().size());
        assertRecordEquals(result.records().get(0), GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member2UpdatedEpoch));

        // Member 2 rejoins the group with the same instance id.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> rejoinResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(member2RejoinId)
                .setGroupId(groupId)
                .setInstanceId("instance-id-2")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar")) // bar is new.
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member2RejoinId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            rejoinResult.response()
        );

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setInstanceId("instance-id-2")
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)))
            .build();

        ConsumerGroupMember expectedRejoinedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setInstanceId("instance-id-2")
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
            .build();

        List<CoordinatorRecord> expectedRecordsAfterRejoin = List.of(
            // The previous member is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),

            // The new member is created as a copy of the previous one but
            // with its new member id and new epochs.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, member2RejoinId, mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember),

            // As the new member as a different subscribed topic set, a rebalance is triggered.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedRejoinedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, member2RejoinId, mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedRejoinedMember)
        );

        assertRecordsEquals(expectedRecordsAfterRejoin, rejoinResult.records());
        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId2);
        context.assertNoRebalanceTimeout(groupId, memberId2);
    }

    @Test
    public void testNoGroupEpochBumpWhenStaticMemberTemporarilyLeaves() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId1)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId2)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            // Use zar only here to ensure that metadata needs to be recomputed.
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // member epoch of the response would be set to -2
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH),
            result.response()
        );

        ConsumerGroupMember member2UpdatedEpoch = new ConsumerGroupMember
            .Builder(member2)
            .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
            .build();

        assertEquals(1, result.records().size());
        assertRecordEquals(result.records().get(0), GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member2UpdatedEpoch));
    }

    @Test
    public void testLeavingStaticMemberBumpsGroupEpoch() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setInstanceId(memberId2)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH),
            result.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Subscription metadata is recomputed because zar is no longer there.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testShouldThrownUnreleasedInstanceIdExceptionWhenNewMemberJoinsWithInUseInstanceId() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 joins the consumer group with an in-use instance id.
        assertThrows(UnreleasedInstanceIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId1)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testShouldThrownUnknownMemberIdExceptionWhenUnknownStaticMemberJoins() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 joins the consumer group with a non-zero epoch
        assertThrows(UnknownMemberIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId2)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testShouldThrowFencedInstanceIdExceptionWhenStaticMemberWithDifferentMemberIdJoins() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-" + memberId1)
                .setInstanceId(memberId1)
                .setMemberEpoch(11)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupMemberEpochValidationForStaticMember() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3)
        )));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 100));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        // Member epoch is greater than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(200)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member epoch is smaller than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(50)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch but without providing partitions.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(99)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch and has a subset of the owned partitions. This
        // is accepted as the response with the bumped epoch may have been lost. In this
        // case, we provide back the correct epoch to the member.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setInstanceId(memberId)
                .setMemberEpoch(99)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(1, 2)))));
        assertEquals(100, result.response().memberEpoch());
    }

    @Test
    public void testShouldThrowUnknownMemberIdExceptionWhenUnknownStaticMemberLeaves() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setInstanceId("unknown-" + memberId1)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testShouldThrowFencedInstanceIdExceptionWhenStaticMemberWithDifferentMemberIdLeaves() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-" + memberId1)
                .setInstanceId(memberId1)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupHeartbeatFullResponse() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with an empty consumer group.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addRacks()
                .build())
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1))))
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result;

        // A full response should be sent back on joining.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // Otherwise, a partial response should be sent back.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        // A full response should be sent back when the member sends
        // a full request again with topic names set.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch())
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // A full response should be sent back when the member sends
        // a full request again with regex set.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch())
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo.*")
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );
    }

    @Test
    public void testReconciliationProcess() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 2)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 1)
            ))
        )));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result;

        // Members in the group are in Stable state.
        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));

        // Member 3 joins the group. This triggers the computation of a new target assignment
        // for the group. Member 3 does not get any assigned partitions yet because they are
        // all owned by other members. However, it transitions to epoch 11 and the
        // Unreleased Partitions state.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        // We only check the last record as the subscription/target assignment updates are
        // already covered by other tests.
        assertRecordEquals(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 heartbeats. It remains at epoch 10 but transitions to Unrevoked Partitions
        // state until it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0))
                    ))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2),
                    mkTopicAssignment(barTopicId, 1)))
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats. It remains at epoch 10 but transitions to Unrevoked Partitions
        // state until it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(2))
                    ))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3),
                    mkTopicAssignment(barTopicId, 2)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5)))
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. The response does not contain any assignment
        // because the member is still waiting on other members to revoke partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10)
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0, 1)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(List.of(0))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)))
                .build())),
            result.records()
        );

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats but without acknowledging the revocation yet. This is basically a no-op.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. It receives the partitions revoked by member 1 but remains
        // in Unreleased Partitions state because it still waits on other partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(1))))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(barTopicId, 1)))
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. Member 2 has not acknowledged the revocation of its partition so
        // member keeps its current assignment.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10)
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(List.of(2))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(2, 3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(2))
                    ))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2, 3),
                    mkTopicAssignment(barTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats to acknowledge its current assignment. It receives all its partitions and
        // transitions to Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11)
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(List.of(1)))));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(1))))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5),
                    mkTopicAssignment(barTopicId, 1)))
                .build())),
            result.records()
        );

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testNewMemberIsRejectedWithMaximumMembersIsReached() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, 2)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(GroupMaxSizeReachedException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId3)
                    .setMemberEpoch(0)
                    .setServerAssignor("range")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupStates() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11));

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testPartitionAssignorExceptionOnRegularHeartbeat() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        ConsumerGroupPartitionAssignor assignor = mock(ConsumerGroupPartitionAssignor.class);
        when(assignor.name()).thenReturn("range");
        when(assignor.assign(any(), any())).thenThrow(new PartitionAssignorException("Assignment failed."));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .build();

        // Member 1 joins the consumer group. The request fails because the
        // target assignment computation failed.
        assertThrows(UnknownServerException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignor("range")
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testSubscriptionMetadataRefreshedAfterGroupIsLoaded() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withSubscriptionMetadata(
                    // foo only has 3 partitions stored in the metadata but foo has
                    // 6 partitions the metadata image.
                    Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 3))
                ))
            .build();

        // The metadata refresh flag should be true.
        ConsumerGroup consumerGroup = context.groupMetadataManager
            .consumerGroup(groupId);
        assertTrue(consumerGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The member gets partitions 3, 4 and 5 assigned.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId,
                Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6))
            ),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testSubscriptionMetadataRefreshedAgainAfterWriteFailure() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withSubscriptionMetadata(
                    // foo only has 3 partitions stored in the metadata but foo has
                    // 6 partitions the metadata image.
                    Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 3))
                ))
            .build();

        // The metadata refresh flag should be true.
        ConsumerGroup consumerGroup = context.groupMetadataManager
            .consumerGroup(groupId);
        assertTrue(consumerGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Heartbeat.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The metadata refresh flag is set to a future time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);

        // Rollback the uncommitted changes. This does not rollback the metadata flag
        // because it is not using a timeline data structure.
        context.rollback();

        // However, the next heartbeat should detect the divergence based on the epoch and trigger
        // a metadata refresh.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));


        // The member gets partitions 3, 4 and 5 assigned.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId,
                Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6))
            ),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testGroupIdsByTopics() {
        String groupId1 = "group1";
        String groupId2 = "group2";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 1 subscribes to foo and bar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to foo, bar and zar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to bar and zar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(List.of("bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo and bar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 1 is removed.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId1, "group1-m1"));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId1, "group1-m1"));

        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to nothing.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(List.of("foo"))
                .build()));

        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to nothing.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to nothing.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));
    }

    @Test
    public void testOnNewMetadataImageWithEmptyDelta() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new MockPartitionAssignor("range")))
            .build();

        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        context.groupMetadataManager.onNewMetadataImage(image, delta);
        assertEquals(image, context.groupMetadataManager.image());
    }

    @Test
    public void testOnNewMetadataImage() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // M1 in group 1 subscribes to a and b.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group1",
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(List.of("a", "b"))
                .build()));

        // M1 in group 2 subscribes to b and c.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group2",
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of("b", "c"))
                .build()));

        // M1 in group 3 subscribes to d.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group3",
            new ConsumerGroupMember.Builder("group3-m1")
                .setSubscribedTopicNames(List.of("d"))
                .build()));

        // M1 in group 4 subscribes to e.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group4",
            new ConsumerGroupMember.Builder("group4-m1")
                .setSubscribedTopicNames(List.of("e"))
                .build()));

        // M1 in group 5 subscribes to f.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group5",
            new ConsumerGroupMember.Builder("group5-m1")
                .setSubscribedTopicNames(List.of("f"))
                .build()));

        // Ensures that all refresh flags are set to the future.
        List.of("group1", "group2", "group3", "group4", "group5").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
            group.setMetadataRefreshDeadline(context.time.milliseconds() + 5000L, 0);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Update the metadata image.
        Uuid topicA = Uuid.randomUuid();
        Uuid topicB = Uuid.randomUuid();
        Uuid topicC = Uuid.randomUuid();
        Uuid topicD = Uuid.randomUuid();
        Uuid topicE = Uuid.randomUuid();

        // Create a first base image with topic a, b, c and d.
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(topicA).setName("a"));
        delta.replay(new PartitionRecord().setTopicId(topicA).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicB).setName("b"));
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicC).setName("c"));
        delta.replay(new PartitionRecord().setTopicId(topicC).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicD).setName("d"));
        delta.replay(new PartitionRecord().setTopicId(topicD).setPartitionId(0));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        // Create a delta which updates topic B, deletes topic D and creates topic E.
        delta = new MetadataDelta(image);
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(2));
        delta.replay(new RemoveTopicRecord().setTopicId(topicD));
        delta.replay(new TopicRecord().setTopicId(topicE).setName("e"));
        delta.replay(new PartitionRecord().setTopicId(topicE).setPartitionId(1));
        image = delta.apply(MetadataProvenance.EMPTY);

        // Update metadata image with the delta.
        context.groupMetadataManager.onNewMetadataImage(image, delta);

        // Verify the groups.
        List.of("group1", "group2", "group3", "group4").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
            assertTrue(group.hasMetadataExpired(context.time.milliseconds()));
        });

        List.of("group5").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Verify image.
        assertEquals(image, context.groupMetadataManager.image());
    }

    @Test
    public void testSessionTimeoutLifecycle() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(Collections.emptyList()));
        assertEquals(1, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is rescheduled on second heartbeat.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));
        assertEquals(1, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testSessionTimeoutExpiration() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(Collections.emptyList()));
        assertEquals(1, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time past the session timeout.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<Void, CoordinatorRecord>(
                groupSessionTimeoutKey(groupId, memberId),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Collections.emptyMap()),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testSessionTimeoutExpirationStaticMember() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(Collections.emptyList()));
        assertEquals(1, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Static member sends a temporary leave group request
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setInstanceId(memberId)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(90000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(Collections.emptyList()));

        assertEquals(-2, result.response().memberEpoch());

        // Verify that there is still a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time past the session timeout. No static member joined back as a replacement
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<Void, CoordinatorRecord>(
                groupSessionTimeoutKey(groupId, memberId),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Collections.emptyMap()),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testRebalanceTimeoutLifecycle() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(memberId1, new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1, 2)
        )))));

        // Member 1 joins the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(180000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2)
            ))
        )));

        // Member 2 joins the group.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(90000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to unrevoked partitions. The rebalance timeout
        // is scheduled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setRebalanceTimeoutMs(12000)
                .setSubscribedTopicNames(List.of("foo")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // Verify that there is a revocation timeout. Keep a reference
        // to the timeout for later.
        ScheduledTimeout<Void, CoordinatorRecord> scheduledTimeout =
            context.assertRebalanceTimeout(groupId, memberId1, 12000);

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 acks the revocation. The revocation timeout is cancelled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0, 1)))));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        // Verify that there is not revocation timeout.
        context.assertNoRebalanceTimeout(groupId, memberId1);

        // Execute the scheduled revocation timeout captured earlier to simulate a
        // stale timeout. This should be a no-op.
        assertEquals(Collections.emptyList(), scheduledTimeout.operation.generateRecords().records());
    }

    @Test
    public void testRebalanceTimeoutExpiration() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId1, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2))))
        ));

        // Member 1 joins the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(10000) // Use timeout smaller than session timeout.
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2)
            ))
        )));

        // Member 2 joins the group.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(10000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to revoking. The revocation timeout
        // is scheduled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // Advance time past the revocation timeout.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(10000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<Void, CoordinatorRecord>(
                consumerGroupRebalanceTimeoutKey(groupId, memberId1),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 3)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId1);
        context.assertNoRebalanceTimeout(groupId, memberId1);
    }

    @Test
    public void testOnLoaded() {
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder("foo", 10)
                .withMember(new ConsumerGroupMember.Builder("foo-1")
                    .setState(MemberState.UNREVOKED_PARTITIONS)
                    .setMemberEpoch(9)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .setPartitionsPendingRevocation(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder("foo-2")
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .build())
                .withAssignment("foo-1", mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        // Let's assume that all the records have been replayed and now
        // onLoaded is called to signal it.
        context.groupMetadataManager.onLoaded();

        // All members should have a session timeout in place.
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-1")));
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-2")));

        // foo-1 should also have a revocation timeout in place.
        assertNotNull(context.timer.timeout(consumerGroupRebalanceTimeoutKey("foo", "foo-1")));
    }

    @Test
    public void testUpdateClassicGroupSizeCounter() {
        String groupId0 = "group-0";
        String groupId1 = "group-1";
        String groupId2 = "group-2";
        String groupId3 = "group-3";
        String groupId4 = "group-4";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId0, 10))
            .build();

        ClassicGroup group1 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId1, true);
        ClassicGroup group2 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId2, true);
        ClassicGroup group3 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId3, true);
        ClassicGroup group4 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId4, true);

        context.groupMetadataManager.updateClassicGroupSizeCounter();
        verify(context.metrics, times(1)).setClassicGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(ClassicGroupState.EMPTY, 4L)
        )));

        group1.transitionTo(PREPARING_REBALANCE);
        group2.transitionTo(PREPARING_REBALANCE);
        group2.transitionTo(COMPLETING_REBALANCE);
        group3.transitionTo(PREPARING_REBALANCE);
        group3.transitionTo(COMPLETING_REBALANCE);
        group3.transitionTo(STABLE);
        group4.transitionTo(DEAD);

        context.groupMetadataManager.updateClassicGroupSizeCounter();
        verify(context.metrics, times(1)).setClassicGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(ClassicGroupState.PREPARING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.COMPLETING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.STABLE, 1L),
            Utils.mkEntry(ClassicGroupState.DEAD, 1L)
        )));
    }

    @Test
    public void testGenerateRecordsOnNewClassicGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true);
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());

        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newEmptyGroupMetadataRecord(group, MetadataVersion.latestTesting())),
            joinResult.records
        );
    }

    @Test
    public void testGenerateRecordsOnNewClassicGroupFailureTransformsError() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true);
        assertFalse(joinResult.joinFuture.isDone());

        // Simulate a failed write to the log.
        joinResult.appendFuture.completeExceptionally(new NotLeaderOrFollowerException());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NOT_COORDINATOR.code(), joinResult.joinFuture.get().errorCode());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplayGroupMetadataRecords(boolean useDefaultRebalanceTimeout) {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        byte[] subscription = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            List.of("foo"))).array();
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>();
        List<ClassicGroupMember> expectedMembers = new ArrayList<>();
        JoinGroupRequestProtocolCollection expectedProtocols = new JoinGroupRequestProtocolCollection(0);
        expectedProtocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(subscription));

        IntStream.range(0, 2).forEach(i -> {
            members.add(new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-" + i)
                .setGroupInstanceId("group-instance-id-" + i)
                .setSubscription(subscription)
                .setAssignment(new byte[]{2})
                .setClientId("client-" + i)
                .setClientHost("host-" + i)
                .setSessionTimeout(4000)
                .setRebalanceTimeout(useDefaultRebalanceTimeout ? -1 : 9000)
            );

            expectedMembers.add(new ClassicGroupMember(
                "member-" + i,
                Optional.of("group-instance-id-" + i),
                "client-" + i,
                "host-" + i,
                useDefaultRebalanceTimeout ? 4000 : 9000,
                4000,
                "consumer",
                expectedProtocols,
                new byte[]{2}
            ));
        });

        CoordinatorRecord groupMetadataRecord = GroupMetadataManagerTestContext.newGroupMetadataRecord("group-id",
            new GroupMetadataValue()
                .setMembers(members)
                .setGeneration(1)
                .setLeader("member-0")
                .setProtocolType("consumer")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latestTesting());

        context.replay(groupMetadataRecord);
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        ClassicGroup expectedGroup = new ClassicGroup(
            new LogContext(),
            "group-id",
            STABLE,
            context.time,
            1,
            Optional.of("consumer"),
            Optional.of("range"),
            Optional.of("member-0"),
            Optional.of(context.time.milliseconds())
        );
        expectedMembers.forEach(expectedGroup::add);

        assertEquals(expectedGroup.groupId(), group.groupId());
        assertEquals(expectedGroup.generationId(), group.generationId());
        assertEquals(expectedGroup.protocolType(), group.protocolType());
        assertEquals(expectedGroup.protocolName(), group.protocolName());
        assertEquals(expectedGroup.leaderOrNull(), group.leaderOrNull());
        assertEquals(expectedGroup.currentState(), group.currentState());
        assertEquals(expectedGroup.currentStateTimestampOrDefault(), group.currentStateTimestampOrDefault());
        assertEquals(expectedGroup.currentClassicGroupMembers(), group.currentClassicGroupMembers());
    }

    @Test
    public void testOnLoadedExceedGroupMaxSizeTriggersRebalance() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, 1)
            .build();

        byte[] subscription = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            List.of("foo"))).array();
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>();

        IntStream.range(0, 2).forEach(i -> members.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-" + i)
                .setGroupInstanceId("group-instance-id-" + i)
                .setSubscription(subscription)
                .setAssignment(new byte[]{2})
                .setClientId("client-" + i)
                .setClientHost("host-" + i)
                .setSessionTimeout(4000)
                .setRebalanceTimeout(9000)
        ));

        CoordinatorRecord groupMetadataRecord = GroupMetadataManagerTestContext.newGroupMetadataRecord("group-id",
            new GroupMetadataValue()
                .setMembers(members)
                .setGeneration(1)
                .setLeader("member-0")
                .setProtocolType("consumer")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latestTesting());

        context.replay(groupMetadataRecord);
        context.groupMetadataManager.onLoaded();
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(2, group.numMembers());
    }

    @Test
    public void testOnLoadedSchedulesClassicGroupMemberHeartbeats() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        byte[] subscription = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            List.of("foo"))).array();
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>();

        IntStream.range(0, 2).forEach(i -> members.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-" + i)
                .setGroupInstanceId("group-instance-id-" + i)
                .setSubscription(subscription)
                .setAssignment(new byte[]{2})
                .setClientId("client-" + i)
                .setClientHost("host-" + i)
                .setSessionTimeout(4000)
                .setRebalanceTimeout(9000)
            )
        );

        CoordinatorRecord groupMetadataRecord = GroupMetadataManagerTestContext.newGroupMetadataRecord("group-id",
            new GroupMetadataValue()
                .setMembers(members)
                .setGeneration(1)
                .setLeader("member-0")
                .setProtocolType("consumer")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latestTesting());

        context.replay(groupMetadataRecord);
        context.groupMetadataManager.onLoaded();

        IntStream.range(0, 2).forEach(i -> {
            ScheduledTimeout<Void, CoordinatorRecord> timeout = context.timer.timeout(
                classicGroupHeartbeatKey("group-id", "member-1"));

            assertNotNull(timeout);
            assertEquals(context.time.milliseconds() + 4000, timeout.deadlineMs);
        });
    }

    @Test
    public void testJoinGroupShouldReceiveErrorIfGroupOverMaxSize() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, 10)
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withReason("exceed max group size")
            .build();

        IntStream.range(0, 10).forEach(i -> {
            GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
            assertFalse(joinResult.joinFuture.isDone());
            assertTrue(joinResult.records.isEmpty());
        });

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.GROUP_MAX_SIZE_REACHED.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testDynamicMembersJoinGroupWithMaxSizeAndRequiredKnownMember() {
        boolean requiredKnownMemberId = true;
        int groupMaxSize = 10;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, groupMaxSize)
            .withConfig(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 50)
            .build();

        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. Generate member ids. All requests will be accepted
        // as the group is still Empty.
        List<GroupMetadataManagerTestContext.JoinResult> firstRoundJoinResults = IntStream.range(0, groupMaxSize + 1).mapToObj(i -> context.sendClassicGroupJoin(
            request,
            requiredKnownMemberId
        )).collect(Collectors.toList());

        List<String> memberIds = verifyClassicGroupJoinResponses(firstRoundJoinResults, 0, Errors.MEMBER_ID_REQUIRED);
        assertEquals(groupMaxSize + 1, memberIds.size());
        assertEquals(0, group.numMembers());
        assertTrue(group.isInState(EMPTY));
        assertEquals(groupMaxSize + 1, group.numPendingJoinMembers());

        // Second round of join requests with the generated member ids.
        // One of them will fail, reaching group max size.
        List<GroupMetadataManagerTestContext.JoinResult> secondRoundJoinResults = memberIds.stream().map(memberId -> context.sendClassicGroupJoin(
            request.setMemberId(memberId),
            requiredKnownMemberId
        )).collect(Collectors.toList());

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // the join group phase will complete.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));

        verifyClassicGroupJoinResponses(secondRoundJoinResults, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(0, group.numPendingJoinMembers());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Members that were accepted can rejoin while others are rejected in CompletingRebalance state.
        List<GroupMetadataManagerTestContext.JoinResult> thirdRoundJoinResults = memberIds.stream().map(memberId -> context.sendClassicGroupJoin(
            request.setMemberId(memberId),
            requiredKnownMemberId
        )).collect(Collectors.toList());

        verifyClassicGroupJoinResponses(thirdRoundJoinResults, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
    }

    @Test
    public void testDynamicMembersJoinGroupWithMaxSizeAndNotRequiredKnownMember() {
        boolean requiredKnownMemberId = false;
        int groupMaxSize = 10;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, groupMaxSize)
            .withConfig(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 50)
            .build();

        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. This will trigger a rebalance.
        List<GroupMetadataManagerTestContext.JoinResult> firstRoundJoinResults = IntStream.range(0, groupMaxSize + 1).mapToObj(i -> context.sendClassicGroupJoin(
            request,
            requiredKnownMemberId
        )).collect(Collectors.toList());

        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // we will complete the rebalance.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));

        List<String> memberIds = verifyClassicGroupJoinResponses(firstRoundJoinResults, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);

        // Members that were accepted can rejoin while others are rejected in CompletingRebalance state.
        List<GroupMetadataManagerTestContext.JoinResult> secondRoundJoinResults = memberIds.stream().map(memberId -> context.sendClassicGroupJoin(
            request.setMemberId(memberId),
            requiredKnownMemberId
        )).collect(Collectors.toList());

        verifyClassicGroupJoinResponses(secondRoundJoinResults, 10, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testStaticMembersJoinGroupWithMaxSize() {
        int groupMaxSize = 10;

        List<String> groupInstanceIds = IntStream.range(0, groupMaxSize + 1)
            .mapToObj(i -> "instance-id-" + i)
            .collect(Collectors.toList());

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, groupMaxSize)
            .withConfig(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 50)
            .build();

        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. This will trigger a rebalance.
        List<GroupMetadataManagerTestContext.JoinResult> firstRoundJoinResults = groupInstanceIds.stream()
                                                                                                 .map(instanceId -> context.sendClassicGroupJoin(request.setGroupInstanceId(instanceId)))
                                                                                                 .collect(Collectors.toList());

        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // we will complete the rebalance.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));

        List<String> memberIds = verifyClassicGroupJoinResponses(firstRoundJoinResults, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);

        // Members which were accepted can rejoin, others are rejected, while
        // completing rebalance
        List<GroupMetadataManagerTestContext.JoinResult> secondRoundJoinResults =  IntStream.range(0, groupMaxSize + 1).mapToObj(i -> context.sendClassicGroupJoin(
            request
                .setMemberId(memberIds.get(i))
                .setGroupInstanceId(groupInstanceIds.get(i))
        )).collect(Collectors.toList());

        verifyClassicGroupJoinResponses(secondRoundJoinResults, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testDynamicMembersCanRejoinGroupWithMaxSizeWhileRebalancing() {
        boolean requiredKnownMemberId = true;
        int groupMaxSize = 10;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, groupMaxSize)
            .withConfig(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 50)
            .build();

        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // First round of join requests. Generate member ids.
        List<GroupMetadataManagerTestContext.JoinResult> firstRoundJoinResults =  IntStream.range(0, groupMaxSize + 1)
                                                                                           .mapToObj(__ -> context.sendClassicGroupJoin(request, requiredKnownMemberId))
                                                                                           .collect(Collectors.toList());

        assertEquals(0, group.numMembers());
        assertEquals(groupMaxSize + 1, group.numPendingJoinMembers());
        assertTrue(group.isInState(EMPTY));

        List<String> memberIds = verifyClassicGroupJoinResponses(firstRoundJoinResults, 0, Errors.MEMBER_ID_REQUIRED);
        assertEquals(groupMaxSize + 1, memberIds.size());

        // Second round of join requests with the generated member ids.
        // One of them will fail, reaching group max size.
        memberIds.forEach(memberId -> {
            GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
                request.setMemberId(memberId),
                requiredKnownMemberId
            );
            assertTrue(joinResult.records.isEmpty());
        });

        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Members can rejoin while rebalancing
        List<GroupMetadataManagerTestContext.JoinResult> thirdRoundJoinResults = memberIds.stream().map(memberId -> context.sendClassicGroupJoin(
            request.setMemberId(memberId),
            requiredKnownMemberId
        )).collect(Collectors.toList());

        // Advance clock by group initial rebalance delay to complete first inital delayed join.
        // This will extend the initial rebalance as new members have joined.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));
        // Advance clock by group initial rebalance delay to complete second inital delayed join.
        // Since there are no new members that joined since the previous delayed join,
        // we will complete the rebalance.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(50));

        verifyClassicGroupJoinResponses(thirdRoundJoinResults, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);
        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testLastJoiningMembersAreKickedOutWhenRejoiningGroupWithMaxSize() {
        int groupMaxSize = 10;

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, groupMaxSize)
            .withConfig(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 50)
            .build();

        // Create a group and add members that exceed the group max size.
        ClassicGroup group = context.createClassicGroup("group-id");

        List<String> memberIds = IntStream.range(0, groupMaxSize + 2)
            .mapToObj(i -> group.generateMemberId("client-id", Optional.empty()))
            .collect(Collectors.toList());

        memberIds.forEach(memberId -> group.add(
            new ClassicGroupMember(
                memberId,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                GroupMetadataManagerTestContext.toProtocols("range")
            )
        ));

        context.groupMetadataManager.prepareRebalance(group, "test");

        List<GroupMetadataManagerTestContext.JoinResult> joinResults = memberIds.stream().map(memberId -> context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId)
                .withDefaultProtocolTypeAndProtocols()
                .withRebalanceTimeoutMs(10000)
                .build()
        )).collect(Collectors.toList());

        assertEquals(groupMaxSize, group.numMembers());
        assertEquals(groupMaxSize, group.numAwaitingJoinResponse());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by rebalance timeout to complete join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(10000));

        verifyClassicGroupJoinResponses(joinResults, groupMaxSize, Errors.GROUP_MAX_SIZE_REACHED);

        assertEquals(groupMaxSize, group.numMembers());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        memberIds.subList(groupMaxSize, groupMaxSize + 2)
            .forEach(memberId -> assertFalse(group.hasMember(memberId)));

        memberIds.subList(0, groupMaxSize)
            .forEach(memberId -> assertTrue(group.hasMember(memberId)));
    }

    @Test
    public void testJoinGroupUnknownMemberNewGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId("member-id")
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());

        // Static member
        request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId("member-id")
            .withGroupInstanceId("group-instance-id")
            .build();

        joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testClassicGroupJoinInconsistentProtocolType() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);

        request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("connect")
            .withProtocols(GroupMetadataManagerTestContext.toProtocols("range"))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupWithEmptyProtocolType() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("")
            .withProtocols(GroupMetadataManagerTestContext.toProtocols("range"))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());

        // Send as static member join.
        joinResult = context.sendClassicGroupJoin(request.setGroupInstanceId("group-instance-id"), true, true);
        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupWithEmptyGroupProtocol() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(new JoinGroupRequestProtocolCollection(0))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testNewMemberJoinExpiration() throws Exception {
        // This tests new member expiration during a protracted rebalance. We first create a
        // group with one member which uses a large value for session timeout and rebalance timeout.
        // We then join with one new member and let the rebalance hang while we await the first member.
        // The new member join timeout expires and its JoinGroup request is failed.

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000 + context.classicGroupNewMemberJoinTimeoutMs)
            .withRebalanceTimeoutMs(2 * context.classicGroupNewMemberJoinTimeoutMs)
            .build();

        JoinGroupResponseData firstResponse = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);
        String firstMemberId = firstResponse.memberId();
        assertEquals(firstResponse.leader(), firstMemberId);
        assertEquals(Errors.NONE.code(), firstResponse.errorCode());

        assertNotNull(group);
        assertEquals(0, group.allMembers().stream().filter(ClassicGroupMember::isNew).count());

        // Send second join group request for a new dynamic member.
        GroupMetadataManagerTestContext.JoinResult secondJoinResult = context.sendClassicGroupJoin(request
            .setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(5000)
        );
        assertTrue(secondJoinResult.records.isEmpty());
        assertFalse(secondJoinResult.joinFuture.isDone());

        assertEquals(2, group.allMembers().size());
        assertEquals(1, group.allMembers().stream().filter(ClassicGroupMember::isNew).count());

        ClassicGroupMember newMember = group.allMembers().stream().filter(ClassicGroupMember::isNew).findFirst().get();
        assertNotEquals(firstMemberId, newMember.memberId());

        // Advance clock by new member join timeout to expire the second member.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupNewMemberJoinTimeoutMs));

        assertTrue(secondJoinResult.joinFuture.isDone());
        JoinGroupResponseData secondResponse = secondJoinResult.joinFuture.get();

        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), secondResponse.errorCode());
        assertEquals(1, group.allMembers().size());
        assertEquals(0, group.allMembers().stream().filter(ClassicGroupMember::isNew).count());
        assertEquals(firstMemberId, group.allMembers().iterator().next().memberId());
    }

    @Test
    public void testJoinGroupInconsistentGroupProtocol() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(GroupMetadataManagerTestContext.toProtocols("range"))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.JoinResult otherJoinResult = context.sendClassicGroupJoin(request.setProtocols(GroupMetadataManagerTestContext.toProtocols("roundrobin")));

        assertTrue(joinResult.records.isEmpty());
        assertTrue(otherJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs));
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), otherJoinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupSecondJoinInconsistentProtocol() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());

        // Sending an inconsistent protocol should be refused
        String memberId = joinResult.joinFuture.get().memberId();
        JoinGroupRequestProtocolCollection emptyProtocols = new JoinGroupRequestProtocolCollection(0);
        request = request.setMemberId(memberId)
            .setProtocols(emptyProtocols);

        joinResult = context.sendClassicGroupJoin(request, true);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());

        // Sending consistent protocol should be accepted
        joinResult = context.sendClassicGroupJoin(request.setProtocols(GroupMetadataManagerTestContext.toProtocols("range")), true);

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs));

        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberJoinAsFirstMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .build();

        context.joinClassicGroupAndCompleteJoin(request, false, true);
    }

    @Test
    public void testStaticMemberRejoinWithExplicitUnknownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .withRebalanceTimeoutMs(5000)
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAndCompleteJoin(request, false, true);
        assertEquals(Errors.NONE.code(), response.errorCode());

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request.setMemberId("unknown-member-id")
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupUnknownConsumerExistingGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .withRebalanceTimeoutMs(5000)
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), response.errorCode());

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request.setMemberId("other-member-id")
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupUnknownConsumerNewDeadGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");
        group.transitionTo(DEAD);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupProtocolTypeIsNotProvidedWhenAnErrorOccurs() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId("member-id")
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());
        assertNull(joinResult.joinFuture.get().protocolType());
    }

    @Test
    public void testJoinGroupReturnsTheProtocolType() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        // Leader joins
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(leaderJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());

        // Member joins
        GroupMetadataManagerTestContext.JoinResult memberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(memberJoinResult.records.isEmpty());
        assertFalse(memberJoinResult.joinFuture.isDone());

        // Complete join group phase
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs));
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(memberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals("consumer", leaderJoinResult.joinFuture.get().protocolType());
        assertEquals(Errors.NONE.code(), memberJoinResult.joinFuture.get().errorCode());
        assertEquals("consumer", memberJoinResult.joinFuture.get().protocolType());
    }

    @Test
    public void testDelayInitialRebalanceByGroupInitialRebalanceDelayOnEmptyGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs / 2));
        assertFalse(joinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs / 2 + 1));
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testResetRebalanceDelayWhenNewMemberJoinsGroupDuringInitialRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(context.classicGroupInitialRebalanceDelayMs * 3)
            .build();

        GroupMetadataManagerTestContext.JoinResult firstMemberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(firstMemberJoinResult.records.isEmpty());
        assertFalse(firstMemberJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs - 1));
        GroupMetadataManagerTestContext.JoinResult secondMemberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(secondMemberJoinResult.records.isEmpty());
        assertFalse(secondMemberJoinResult.joinFuture.isDone());
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2));

        // Advance clock past initial rebalance delay and verify futures are not completed.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs / 2 + 1));
        assertFalse(firstMemberJoinResult.joinFuture.isDone());
        assertFalse(secondMemberJoinResult.joinFuture.isDone());

        // Advance clock beyond recomputed delay and make sure the futures have completed.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs / 2));
        assertTrue(firstMemberJoinResult.joinFuture.isDone());
        assertTrue(secondMemberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), firstMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), secondMemberJoinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testDelayRebalanceUptoRebalanceTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(context.classicGroupInitialRebalanceDelayMs * 2)
            .build();

        GroupMetadataManagerTestContext.JoinResult firstMemberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(firstMemberJoinResult.records.isEmpty());
        assertFalse(firstMemberJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.JoinResult secondMemberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(secondMemberJoinResult.records.isEmpty());
        assertFalse(secondMemberJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs + 1));

        GroupMetadataManagerTestContext.JoinResult thirdMemberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(thirdMemberJoinResult.records.isEmpty());
        assertFalse(thirdMemberJoinResult.joinFuture.isDone());

        // Advance clock right before rebalance timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs - 1));
        assertFalse(firstMemberJoinResult.joinFuture.isDone());
        assertFalse(secondMemberJoinResult.joinFuture.isDone());
        assertFalse(thirdMemberJoinResult.joinFuture.isDone());

        // Advance clock beyond rebalance timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(1));
        assertTrue(firstMemberJoinResult.joinFuture.isDone());
        assertTrue(secondMemberJoinResult.joinFuture.isDone());
        assertTrue(thirdMemberJoinResult.joinFuture.isDone());

        assertEquals(Errors.NONE.code(), firstMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), secondMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), thirdMemberJoinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupReplaceStaticMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .build();

        // Send join group as static member.
        GroupMetadataManagerTestContext.JoinResult oldMemberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(oldMemberJoinResult.records.isEmpty());
        assertFalse(oldMemberJoinResult.joinFuture.isDone());
        assertEquals(1, group.numAwaitingJoinResponse());
        assertEquals(1, group.numMembers());

        // Replace static member with new member id. Old member id should be fenced.
        GroupMetadataManagerTestContext.JoinResult newMemberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(newMemberJoinResult.records.isEmpty());
        assertFalse(newMemberJoinResult.joinFuture.isDone());
        assertTrue(oldMemberJoinResult.joinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(1, group.numAwaitingJoinResponse());
        assertEquals(1, group.numMembers());

        // Complete join for new member.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs));
        assertTrue(newMemberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), newMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(0, group.numAwaitingJoinResponse());
        assertEquals(1, group.numMembers());
    }

    @Test
    public void testHeartbeatExpirationShouldRemovePendingMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(1000)
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(0, group.numMembers());
        assertEquals(1, group.numPendingJoinMembers());

        // Advance clock by session timeout. Pending member should be removed from group as heartbeat expires.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(1000));
        assertEquals(0, group.numPendingJoinMembers());
    }

    @Test
    public void testHeartbeatExpirationShouldRemoveMember() throws Exception {
        // Set initial rebalance delay to simulate a long running rebalance.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 10 * 60 * 1000)
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());
        assertEquals(1, group.numMembers());

        String memberId = group.leaderOrNull();
        // Advance clock by new member join timeout. Member should be removed from group as heartbeat expires.
        // A group that transitions to Empty after completing join phase will generate records.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(context.classicGroupNewMemberJoinTimeoutMs);

        assertEquals(1, timeouts.size());
        timeouts.forEach(timeout -> {
            assertEquals(classicGroupHeartbeatKey("group-id", memberId), timeout.key);
            assertEquals(
                List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
                timeout.result.records()
            );
        });

        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(0, group.numMembers());
    }

    @Test
    public void testExistingMemberJoinDeadGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();

        assertTrue(group.hasMember(memberId));

        group.transitionTo(DEAD);

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupExistingPendingMemberWithGroupInstanceIdThrowsException() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true);

        assertTrue(joinResult.records.isEmpty());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());
        assertTrue(joinResult.joinFuture.isDone());
        String memberId = joinResult.joinFuture.get().memberId();

        assertThrows(IllegalStateException.class,
            () -> context.sendClassicGroupJoin(
                request
                    .setMemberId(memberId)
                    .setGroupInstanceId("group-instance-id")
            )
        );
    }

    @Test
    public void testJoinGroupExistingMemberUpdatedMetadataTriggersRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols("range");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(protocols)
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);

        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();
        ClassicGroupMember member = group.member(memberId);

        assertEquals(protocols, member.supportedProtocols());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(1, group.generationId());

        protocols = GroupMetadataManagerTestContext.toProtocols("range", "roundrobin");

        // Send updated member metadata. This should trigger a rebalance and complete the join phase.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(memberId)
                .setProtocols(protocols)
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());

        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.generationId());
        assertEquals(protocols, member.supportedProtocols());
    }

    @Test
    public void testJoinGroupAsExistingLeaderTriggersRebalanceInStableState() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);

        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();

        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertTrue(group.isLeader(memberId));
        assertEquals(1, group.generationId());

        group.transitionTo(STABLE);
        // Sending join group as leader should trigger a rebalance.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request.setMemberId(memberId)
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.generationId());
    }

    @Test
    public void testJoinGroupAsExistingMemberWithUpdatedMetadataTriggersRebalanceInStableState() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");


        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(GroupMetadataManagerTestContext.toProtocols("range"))
            .build();

        JoinGroupResponseData leaderResponse = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), leaderResponse.errorCode());
        String leaderId = leaderResponse.leader();
        assertEquals(1, group.generationId());

        // Member joins.
        GroupMetadataManagerTestContext.JoinResult memberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(memberJoinResult.records.isEmpty());
        assertFalse(memberJoinResult.joinFuture.isDone());

        // Leader also rejoins. Completes join group phase.
        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(request.setMemberId(leaderId));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(memberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), memberJoinResult.joinFuture.get().errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.numMembers());
        assertEquals(2, group.generationId());

        group.transitionTo(STABLE);

        // Member rejoins with updated metadata. This should trigger a rebalance.
        String memberId = memberJoinResult.joinFuture.get().memberId();

        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols("range", "roundrobin");

        memberJoinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(memberId)
                .setProtocols(protocols)
        );

        assertTrue(memberJoinResult.records.isEmpty());
        assertFalse(memberJoinResult.joinFuture.isDone());

        // Leader rejoins. This completes the join group phase.
        leaderJoinResult = context.sendClassicGroupJoin(request.setMemberId(leaderId));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(memberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), memberJoinResult.joinFuture.get().errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(3, group.generationId());
        assertEquals(2, group.numMembers());
    }

    @Test
    public void testJoinGroupExistingMemberDoesNotTriggerRebalanceInStableState() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData leaderResponse = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), leaderResponse.errorCode());
        String leaderId = leaderResponse.leader();
        assertEquals(1, group.generationId());

        // Member joins.
        GroupMetadataManagerTestContext.JoinResult memberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(memberJoinResult.records.isEmpty());
        assertFalse(memberJoinResult.joinFuture.isDone());

        // Leader also rejoins. Completes join group phase.
        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(request.setMemberId(leaderId));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(memberJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.numMembers());
        assertEquals(2, group.generationId());

        String memberId = memberJoinResult.joinFuture.get().memberId();

        group.transitionTo(STABLE);

        // Member rejoins with no metadata changes. This does not trigger a rebalance.
        memberJoinResult = context.sendClassicGroupJoin(request.setMemberId(memberId));

        assertTrue(memberJoinResult.records.isEmpty());
        assertTrue(memberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), memberJoinResult.joinFuture.get().errorCode());
        assertEquals(2, memberJoinResult.joinFuture.get().generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testJoinGroupExistingMemberInEmptyState() throws Exception {
        // Existing member joins a group that is in Empty/Dead state. Ask member to rejoin with generation id reset.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);

        assertEquals(Errors.NONE.code(), response.errorCode());
        String memberId = response.memberId();

        assertTrue(group.isInState(COMPLETING_REBALANCE));

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request.setMemberId(memberId));

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(-1, joinResult.joinFuture.get().generationId());
    }

    @Test
    public void testCompleteJoinRemoveNotYetRejoinedDynamicMembers() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(1000)
            .withRebalanceTimeoutMs(1000)
            .build();

        JoinGroupResponseData leaderResponse = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(request);
        assertEquals(Errors.NONE.code(), leaderResponse.errorCode());
        assertEquals(1, group.generationId());

        // Add new member. This triggers a rebalance.
        GroupMetadataManagerTestContext.JoinResult memberJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(memberJoinResult.records.isEmpty());
        assertFalse(memberJoinResult.joinFuture.isDone());
        assertEquals(2, group.numMembers());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by rebalance timeout. This will expire the leader as it has not rejoined.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(1000));

        assertTrue(memberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), memberJoinResult.joinFuture.get().errorCode());
        assertEquals(1, group.numMembers());
        assertTrue(group.hasMember(memberJoinResult.joinFuture.get().memberId()));
        assertEquals(2, group.generationId());
    }

    @Test
    public void testCompleteJoinPhaseInEmptyStateSkipsRebalance() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(1000)
            .withRebalanceTimeoutMs(1000)
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());

        assertEquals(0, group.generationId());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        group.transitionTo(DEAD);

        // Advance clock by initial rebalance delay to complete join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs));
        assertEquals(0, group.generationId());
    }

    @Test
    public void testCompleteJoinPhaseNoMembersRejoinedExtendsJoinPhase() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("first-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(30000)
            .withRebalanceTimeoutMs(10000)
            .build();

        // First member joins group and completes join phase.
        JoinGroupResponseData firstMemberResponse = context.joinClassicGroupAndCompleteJoin(request, true, true);
        assertEquals(Errors.NONE.code(), firstMemberResponse.errorCode());
        String firstMemberId = firstMemberResponse.memberId();

        // Second member joins and group goes into rebalancing state.
        GroupMetadataManagerTestContext.JoinResult secondMemberJoinResult = context.sendClassicGroupJoin(
            request.setGroupInstanceId("second-instance-id")
        );

        assertTrue(secondMemberJoinResult.records.isEmpty());
        assertFalse(secondMemberJoinResult.joinFuture.isDone());

        // First static member rejoins and completes join phase.
        GroupMetadataManagerTestContext.JoinResult firstMemberJoinResult = context.sendClassicGroupJoin(
            request.setMemberId(firstMemberId).setGroupInstanceId("first-instance-id"));

        assertTrue(firstMemberJoinResult.records.isEmpty());
        assertTrue(firstMemberJoinResult.joinFuture.isDone());
        assertTrue(secondMemberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), firstMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), secondMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(2, group.numMembers());
        assertEquals(2, group.generationId());

        String secondMemberId = secondMemberJoinResult.joinFuture.get().memberId();

        // Trigger a rebalance. No members rejoined.
        context.groupMetadataManager.prepareRebalance(group, "trigger rebalance");

        assertEquals(2, group.numMembers());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(0, group.numAwaitingJoinResponse());

        // Advance clock by rebalance timeout to complete join phase. As long as both members have not
        // rejoined, we extend the join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(10000));
        assertEquals(10000, context.timer.timeout("join-group-id").deadlineMs - context.time.milliseconds());
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(10000));
        assertEquals(10000, context.timer.timeout("join-group-id").deadlineMs - context.time.milliseconds());

        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(2, group.numMembers());
        assertEquals(2, group.generationId());

        // Let first and second member rejoin. This should complete the join phase.
        firstMemberJoinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(firstMemberId)
                .setGroupInstanceId("first-instance-id")
        );

        assertTrue(firstMemberJoinResult.records.isEmpty());
        assertFalse(firstMemberJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertEquals(2, group.numMembers());
        assertEquals(2, group.generationId());

        secondMemberJoinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(secondMemberId)
                .setGroupInstanceId("second-instance-id")
        );

        assertTrue(secondMemberJoinResult.records.isEmpty());
        assertTrue(firstMemberJoinResult.joinFuture.isDone());
        assertTrue(secondMemberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), firstMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), secondMemberJoinResult.joinFuture.get().errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.numMembers());
        assertEquals(3, group.generationId());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplaceStaticMemberInStableStateNoError(
        boolean supportSkippingAssignment
    ) throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(GroupMetadataManagerTestContext.toProtocols("range"))
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAndCompleteJoin(request, true, supportSkippingAssignment);
        assertEquals(Errors.NONE.code(), response.errorCode());
        String oldMemberId = response.memberId();

        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID. This should update the log with the generated member id.
        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols("range", "roundrobin");

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request
                .setProtocols(protocols)
                .setRebalanceTimeoutMs(7000)
                .setSessionTimeoutMs(4500),
            true,
            supportSkippingAssignment
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            joinResult.records
        );
        assertFalse(joinResult.joinFuture.isDone());

        // Write was successful.
        joinResult.appendFuture.complete(null);
        assertTrue(joinResult.joinFuture.isDone());

        String newMemberId = group.staticMemberId("group-instance-id");

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setMembers(Collections.emptyList())
            .setLeader(oldMemberId)
            .setMemberId(newMemberId)
            .setGenerationId(1)
            .setProtocolType("consumer")
            .setProtocolName("range")
            .setSkipAssignment(supportSkippingAssignment)
            .setErrorCode(Errors.NONE.code());

        if (supportSkippingAssignment) {
            expectedResponse
                .setMembers(List.of(
                    new JoinGroupResponseData.JoinGroupResponseMember()
                        .setMemberId(newMemberId)
                        .setGroupInstanceId("group-instance-id")
                        .setMetadata(protocols.find("range").metadata())
                    ))
                .setLeader(newMemberId);
        }

        ClassicGroupMember updatedMember = group.member(group.staticMemberId("group-instance-id"));

        assertEquals(expectedResponse, joinResult.joinFuture.get());
        assertEquals(newMemberId, updatedMember.memberId());
        assertEquals(Optional.of("group-instance-id"), updatedMember.groupInstanceId());
        assertEquals(7000, updatedMember.rebalanceTimeoutMs());
        assertEquals(4500, updatedMember.sessionTimeoutMs());
        assertEquals(protocols, updatedMember.supportedProtocols());

        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testReplaceStaticMemberInStableStateWithUpdatedProtocolTriggersRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(GroupMetadataManagerTestContext.toProtocols("range", "roundrobin"))
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAndCompleteJoin(request, true, true);
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID. The selected protocol changes and triggers a rebalance.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request.setProtocols(GroupMetadataManagerTestContext.toProtocols("roundrobin"))
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(1, group.numMembers());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testReplaceStaticMemberInStableStateErrors() throws Exception {
        // If the append future fails, confirm that the member is not updated.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols("range");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(protocols)
            .withRebalanceTimeoutMs(4000)
            .withSessionTimeoutMs(3000)
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAndCompleteJoin(request, false, false);
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        String oldMemberId = response.memberId();
        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID but the append fails. The group should not be updated.
        protocols.add(new JoinGroupRequestProtocol()
                .setName("roundrobin")
                .setMetadata(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of("bar"))).array()));

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request
                .setProtocols(protocols)
                .setRebalanceTimeoutMs(7000)
                .setSessionTimeoutMs(6000),
            false,
            false
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            joinResult.records
        );
        assertFalse(joinResult.joinFuture.isDone());

        // Simulate a failed write to the log.
        joinResult.appendFuture.completeExceptionally(new UnknownTopicOrPartitionException());
        assertTrue(joinResult.joinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setMembers(Collections.emptyList())
            .setLeader(oldMemberId)
            .setMemberId(UNKNOWN_MEMBER_ID)
            .setGenerationId(1)
            .setProtocolType("consumer")
            .setProtocolName("range")
            .setSkipAssignment(false)
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());

        assertEquals(expectedResponse, joinResult.joinFuture.get());

        ClassicGroupMember revertedMember = group.member(group.staticMemberId("group-instance-id"));

        assertEquals(oldMemberId, revertedMember.memberId());
        assertEquals(Optional.of("group-instance-id"), revertedMember.groupInstanceId());
        assertEquals(4000, revertedMember.rebalanceTimeoutMs());
        assertEquals(3000, revertedMember.sessionTimeoutMs());
        assertEquals(protocols, revertedMember.supportedProtocols());
        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplaceStaticMemberInStableStateSucceeds(
        boolean supportSkippingAssignment
    ) throws Exception {
        // If the append future succeeds, the soft state is updated with the new member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(GroupMetadataManagerTestContext.toProtocols("range"))
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAndCompleteJoin(
            request,
            true,
            supportSkippingAssignment
        );

        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        String oldMemberId = response.memberId();
        // Simulate successful sync group phase
        group.transitionTo(STABLE);

        // Static member rejoins with UNKNOWN_MEMBER_ID and the append succeeds.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request
                .setProtocols(GroupMetadataManagerTestContext.toProtocols("range", "roundrobin"))
                .setRebalanceTimeoutMs(7000)
                .setSessionTimeoutMs(6000),
            true,
            supportSkippingAssignment);

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            joinResult.records
        );
        assertFalse(joinResult.joinFuture.isDone());

        // Simulate a successful write to the log.
        joinResult.appendFuture.complete(null);
        assertTrue(joinResult.joinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setMembers(supportSkippingAssignment ? toJoinResponseMembers(group) : Collections.emptyList())
            .setLeader(supportSkippingAssignment ? joinResult.joinFuture.get().memberId() : oldMemberId)
            .setMemberId(joinResult.joinFuture.get().memberId())
            .setGenerationId(1)
            .setProtocolType("consumer")
            .setProtocolName("range")
            .setSkipAssignment(supportSkippingAssignment)
            .setErrorCode(Errors.NONE.code());

        assertEquals(expectedResponse, joinResult.joinFuture.get());

        ClassicGroupMember newMember = group.member(group.staticMemberId("group-instance-id"));

        assertNotEquals(oldMemberId, newMember.memberId());
        assertEquals(Optional.of("group-instance-id"), newMember.groupInstanceId());
        assertEquals(7000, newMember.rebalanceTimeoutMs());
        assertEquals(6000, newMember.sessionTimeoutMs());
        assertEquals(GroupMetadataManagerTestContext.toProtocols("range", "roundrobin"), newMember.supportedProtocols());
        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testReplaceStaticMemberInCompletingRebalanceStateTriggersRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("group-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData response = context.joinClassicGroupAndCompleteJoin(request, true, true);
        assertEquals(Errors.NONE.code(), response.errorCode());

        assertEquals(1, group.numMembers());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Static member rejoins with UNKNOWN_MEMBER_ID and triggers a rebalance.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(1, group.numMembers());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testJoinGroupAppendErrorConversion() {
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, appendGroupMetadataErrorToResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION));
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, appendGroupMetadataErrorToResponseError(Errors.NOT_ENOUGH_REPLICAS));
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, appendGroupMetadataErrorToResponseError(Errors.REQUEST_TIMED_OUT));

        assertEquals(Errors.NOT_COORDINATOR, appendGroupMetadataErrorToResponseError(Errors.NOT_LEADER_OR_FOLLOWER));
        assertEquals(Errors.NOT_COORDINATOR, appendGroupMetadataErrorToResponseError(Errors.KAFKA_STORAGE_ERROR));

        assertEquals(Errors.UNKNOWN_SERVER_ERROR, appendGroupMetadataErrorToResponseError(Errors.MESSAGE_TOO_LARGE));
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, appendGroupMetadataErrorToResponseError(Errors.RECORD_LIST_TOO_LARGE));
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, appendGroupMetadataErrorToResponseError(Errors.INVALID_FETCH_SIZE));

        assertEquals(Errors.LEADER_NOT_AVAILABLE, Errors.LEADER_NOT_AVAILABLE);
    }

    @Test
    public void testNewMemberTimeoutCompletion() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .withSessionTimeoutMs(context.classicGroupNewMemberJoinTimeoutMs + 5000)
                .build()
        );

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());

        // Advance clock by initial rebalance delay to complete join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupInitialRebalanceDelayMs));

        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        assertEquals(0, group.allMembers().stream().filter(ClassicGroupMember::isNew).count());

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(joinResult.joinFuture.get().memberId())
                .withGenerationId(joinResult.joinFuture.get().generationId())
                .build()
        );

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(1, group.numMembers());

        // Make sure the NewMemberTimeout is not still in effect, and the member is not kicked
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(context.classicGroupNewMemberJoinTimeoutMs));
        assertEquals(1, group.numMembers());

        // Member should be removed as heartbeat expires. The group is now empty.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(5000);
        List<CoordinatorRecord> expectedRecords = List.of(GroupMetadataManagerTestContext.newGroupMetadataRecord(
            group.groupId(),
            new GroupMetadataValue()
                .setMembers(Collections.emptyList())
                .setGeneration(2)
                .setLeader(null)
                .setProtocolType("consumer")
                .setProtocol(null)
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latestTesting())
        );

        assertEquals(1, timeouts.size());
        String memberId = joinResult.joinFuture.get().memberId();
        timeouts.forEach(timeout -> {
            assertEquals(classicGroupHeartbeatKey("group-id", memberId), timeout.key);
            assertEquals(expectedRecords, timeout.result.records());
        });

        assertEquals(0, group.numMembers());
        assertTrue(group.isInState(EMPTY));
    }

    @Test
    public void testNewMemberFailureAfterJoinGroupCompletion() throws Exception {
        // For old versions of the JoinGroup protocol, new members were subject
        // to expiration if the rebalance took long enough. This test case ensures
        // that following completion of the JoinGroup phase, new members follow
        // normal heartbeat expiration logic.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withSessionTimeoutMs(5000)
            .withRebalanceTimeoutMs(10000)
            .build();

        JoinGroupResponseData joinResponse = context.joinClassicGroupAndCompleteJoin(joinRequest, false, false);
        assertEquals(Errors.NONE.code(), joinResponse.errorCode());

        String memberId = joinResponse.memberId();
        assertEquals(memberId, joinResponse.leader());
        assertEquals(1, joinResponse.generationId());

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId)
                .withGenerationId(1)
                .build()
        );

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());

        assertTrue(group.isInState(STABLE));
        assertEquals(1, group.generationId());

        GroupMetadataManagerTestContext.JoinResult otherJoinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(UNKNOWN_MEMBER_ID));
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(memberId));

        assertTrue(otherJoinResult.records.isEmpty());
        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertTrue(otherJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), otherJoinResult.joinFuture.get().errorCode());

        context.verifySessionExpiration(group, 5000);
    }

    @Test
    public void testStaticMemberFenceDuplicateRejoinedFollower() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // A third member joins. Trigger a rebalance.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        context.sendClassicGroupJoin(request);

        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Old follower rejoins group will be matching current member.id.
        GroupMetadataManagerTestContext.JoinResult oldFollowerJoinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(rebalanceResult.followerId)
                .setGroupInstanceId("follower-instance-id")
        );

        assertTrue(oldFollowerJoinResult.records.isEmpty());
        assertFalse(oldFollowerJoinResult.joinFuture.isDone());

        // Duplicate follower joins group with unknown member id will trigger member id replacement.
        context.sendClassicGroupJoin(
            request
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setGroupInstanceId("follower-instance-id")
        );

        // Old member shall be fenced immediately upon duplicate follower joins.
        assertTrue(oldFollowerJoinResult.records.isEmpty());
        assertTrue(oldFollowerJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
            .setProtocolName(null)
            .setProtocolType(null)
            .setLeader(UNKNOWN_MEMBER_ID)
            .setMemberId(rebalanceResult.followerId)
            .setGenerationId(-1);

        checkJoinGroupResponse(
            expectedResponse,
            oldFollowerJoinResult.joinFuture.get(),
            group,
            PREPARING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberFenceDuplicateSyncingFollowerAfterMemberIdChanged() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Known leader rejoins will trigger rebalance.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .withRebalanceTimeoutMs(10000)
            .build();

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(leaderJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Old follower rejoins group will match current member.id.
        GroupMetadataManagerTestContext.JoinResult oldFollowerJoinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(rebalanceResult.followerId)
                .setGroupInstanceId("follower-instance-id")
        );

        assertTrue(oldFollowerJoinResult.records.isEmpty());
        assertTrue(oldFollowerJoinResult.joinFuture.isDone());
        assertTrue(leaderJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.leaderId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id", "follower-instance-id")
        );

        assertEquals(rebalanceResult.leaderId, leaderJoinResult.joinFuture.get().memberId());
        assertEquals(rebalanceResult.leaderId, leaderJoinResult.joinFuture.get().leader());

        // Old follower should get a successful join group response.
        assertTrue(oldFollowerJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(oldFollowerJoinResult.joinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer");

        checkJoinGroupResponse(
            expectedFollowerResponse,
            oldFollowerJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );

        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(rebalanceResult.followerId, oldFollowerJoinResult.joinFuture.get().memberId());
        assertEquals(rebalanceResult.leaderId, oldFollowerJoinResult.joinFuture.get().leader());

        // Duplicate follower joins group with unknown member id will trigger member.id replacement,
        // and will also trigger a rebalance under CompletingRebalance state; the old follower sync callback
        // will return fenced exception while broker replaces the member identity with the duplicate follower joins.
        GroupMetadataManagerTestContext.SyncResult oldFollowerSyncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("follower-instance-id")
                .withGenerationId(oldFollowerJoinResult.joinFuture.get().generationId())
                .withMemberId(oldFollowerJoinResult.joinFuture.get().memberId())
                .build()
        );

        assertTrue(oldFollowerSyncResult.records.isEmpty());
        assertFalse(oldFollowerSyncResult.syncFuture.isDone());

        GroupMetadataManagerTestContext.JoinResult duplicateFollowerJoinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setGroupInstanceId("follower-instance-id")
        );

        assertTrue(duplicateFollowerJoinResult.records.isEmpty());
        assertTrue(group.isInState(PREPARING_REBALANCE));
        assertFalse(duplicateFollowerJoinResult.joinFuture.isDone());
        assertTrue(oldFollowerSyncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldFollowerSyncResult.syncFuture.get().errorCode());

        // Advance clock by rebalance timeout so that the join phase completes with duplicate follower.
        // Both heartbeats will expire but only the leader is kicked out.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(10000);
        assertEquals(2, timeouts.size());
        timeouts.forEach(timeout -> assertEquals(timeout.result, EMPTY_RESULT));

        assertTrue(duplicateFollowerJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(3, group.generationId());
        assertEquals(1, group.numMembers());
        assertTrue(group.hasMember(duplicateFollowerJoinResult.joinFuture.get().memberId()));
        assertEquals(duplicateFollowerJoinResult.joinFuture.get().memberId(), duplicateFollowerJoinResult.joinFuture.get().leader());
    }

    @Test
    public void testStaticMemberFenceDuplicateRejoiningFollowerAfterMemberIdChanged() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Known leader rejoins will trigger rebalance.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .build();

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(request);

        assertTrue(leaderJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Duplicate follower joins group will trigger member id replacement.
        GroupMetadataManagerTestContext.JoinResult duplicateFollowerJoinResult = context.sendClassicGroupJoin(
            request
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setGroupInstanceId("follower-instance-id")
        );

        assertTrue(duplicateFollowerJoinResult.records.isEmpty());
        assertTrue(duplicateFollowerJoinResult.joinFuture.isDone());

        // Old follower rejoins group will fail because member id is already updated.
        GroupMetadataManagerTestContext.JoinResult oldFollowerJoinResult = context.sendClassicGroupJoin(
            request.setMemberId(rebalanceResult.followerId)
        );

        assertTrue(oldFollowerJoinResult.records.isEmpty());
        assertTrue(oldFollowerJoinResult.joinFuture.isDone());
        assertTrue(leaderJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.leaderId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id", "follower-instance-id")
        );

        assertTrue(duplicateFollowerJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedDuplicateFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(duplicateFollowerJoinResult.joinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedDuplicateFollowerResponse,
            duplicateFollowerJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );

        assertTrue(duplicateFollowerJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedOldFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
            .setGenerationId(-1)
            .setMemberId(rebalanceResult.followerId)
            .setLeader(UNKNOWN_MEMBER_ID)
            .setProtocolName(null)
            .setProtocolType(null)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedOldFollowerResponse,
            oldFollowerJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberRejoinWithKnownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId("group-instance-id")
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData joinResponse = context.joinClassicGroupAndCompleteJoin(request, false, false);
        assertEquals(Errors.NONE.code(), joinResponse.errorCode());

        String memberId = joinResponse.memberId();

        GroupMetadataManagerTestContext.JoinResult rejoinResult = context.sendClassicGroupJoin(
            request.setMemberId(memberId)
        );

        // The second join group should return immediately since we are using the same metadata during CompletingRebalance.
        assertTrue(rejoinResult.records.isEmpty());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertTrue(rejoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), rejoinResult.joinFuture.get().errorCode());

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId)
                .withGenerationId(joinResponse.generationId())
                .withGroupInstanceId("group-instance-id")
                .build()
        );

        // Successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStaticMemberRejoinWithLeaderIdAndUnknownMemberId(
        boolean supportSkippingAssignment
    ) throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // A static leader rejoin with unknown id will not trigger rebalance, and no assignment will be returned.
        // As the group was in Stable state and the member id was updated, this will generate records.
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            joinRequest,
            true,
            supportSkippingAssignment
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            joinResult.records
        );
        // Simulate a successful write to the log.
        joinResult.appendFuture.complete(null);
        assertTrue(joinResult.joinFuture.isDone());

        String leader = supportSkippingAssignment ?
            joinResult.joinFuture.get().memberId() : rebalanceResult.leaderId;

        List<JoinGroupResponseMember> members = supportSkippingAssignment ?
            toJoinResponseMembers(group) : Collections.emptyList();

        JoinGroupResponseData expectedJoinResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId)
            .setMemberId(joinResult.joinFuture.get().memberId())
            .setLeader(leader)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(supportSkippingAssignment)
            .setMembers(members);

        checkJoinGroupResponse(
            expectedJoinResponse,
            joinResult.joinFuture.get(),
            group,
            STABLE,
            supportSkippingAssignment ? Set.of("leader-instance-id", "follower-instance-id") : Collections.emptySet()
        );

        GroupMetadataManagerTestContext.JoinResult oldLeaderJoinResult = context.sendClassicGroupJoin(
            joinRequest.setMemberId(rebalanceResult.leaderId),
            true,
            supportSkippingAssignment
        );

        assertTrue(oldLeaderJoinResult.records.isEmpty());
        assertTrue(oldLeaderJoinResult.joinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldLeaderJoinResult.joinFuture.get().errorCode());

        // Old leader will get fenced.
        SyncGroupRequestData oldLeaderSyncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withGenerationId(rebalanceResult.generationId)
            .withMemberId(rebalanceResult.leaderId)
            .build();

        GroupMetadataManagerTestContext.SyncResult oldLeaderSyncResult = context.sendClassicGroupSync(oldLeaderSyncRequest);

        assertTrue(oldLeaderSyncResult.records.isEmpty());
        assertTrue(oldLeaderSyncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), oldLeaderSyncResult.syncFuture.get().errorCode());

        // Calling sync on old leader id will fail because that leader id is no longer valid and replaced.
        SyncGroupRequestData newLeaderSyncRequest = oldLeaderSyncRequest.setGroupInstanceId(null);
        GroupMetadataManagerTestContext.SyncResult newLeaderSyncResult = context.sendClassicGroupSync(newLeaderSyncRequest);

        assertTrue(newLeaderSyncResult.records.isEmpty());
        assertTrue(newLeaderSyncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), newLeaderSyncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithLeaderIdAndKnownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Known static leader rejoin will trigger rebalance.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .build();

        JoinGroupResponseData joinResponse = context.joinClassicGroupAndCompleteJoin(request, true, true, 10000);

        // Follower's heartbeat expires as the leader rejoins.
        assertFalse(group.hasMember(rebalanceResult.followerId));

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.leaderId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedResponse,
            joinResponse,
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinWithLeaderIdAndUnexpectedDeadGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);
        group.transitionTo(DEAD);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true, true);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithLeaderIdAndUnexpectedEmptyGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true, true);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithFollowerIdAndChangeOfProtocol() throws Exception {
        int rebalanceTimeoutMs = 10000;
        int sessionTimeoutMs = 15000;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            rebalanceTimeoutMs,
            sessionTimeoutMs
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // A static follower rejoin with changed protocol will trigger rebalance.
        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols("roundrobin");
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(rebalanceResult.followerId)
            .withProtocols(protocols)
            .withRebalanceTimeoutMs(rebalanceTimeoutMs)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());

        // Old leader hasn't joined in the meantime, triggering a re-election.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs));

        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertTrue(group.hasStaticMember("leader-instance-id"));
        assertTrue(group.isLeader(rebalanceResult.followerId));

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(rebalanceResult.followerId)
            .setLeader(rebalanceResult.followerId)
            .setProtocolName("roundrobin")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedResponse,
            joinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id", "follower-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWithSelectedProtocolChanged() throws Exception {

        int rebalanceTimeoutMs = 10000;
        int sessionTimeoutMs = 15000;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            rebalanceTimeoutMs,
            sessionTimeoutMs
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        assertNotEquals("roundrobin", group.selectProtocol());

        // A static follower rejoin with changed protocol will trigger rebalance.
        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols("roundrobin");

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocols(protocols)
            .withRebalanceTimeoutMs(rebalanceTimeoutMs)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());
        assertEquals("roundrobin", group.selectProtocol());

        // Old leader hasn't joined in the meantime, triggering a re-election.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs));
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertTrue(group.hasStaticMember("leader-instance-id"));
        assertTrue(group.isLeader(joinResult.joinFuture.get().memberId()));
        assertNotEquals(rebalanceResult.followerId, joinResult.joinFuture.get().memberId());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(joinResult.joinFuture.get().memberId())
            .setLeader(joinResult.joinFuture.get().memberId())
            .setProtocolName("roundrobin")
            .setProtocolType("consumer")
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedResponse,
            joinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id", "follower-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchangedPersistenceFailure() throws Exception {

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols(group.selectProtocol());

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocols(protocols)
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            followerJoinResult.records
        );
        // Simulate a failed write to the log.
        followerJoinResult.appendFuture.completeExceptionally(Errors.MESSAGE_TOO_LARGE.exception());
        assertTrue(followerJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
            .setGenerationId(rebalanceResult.generationId)
            .setMemberId(followerJoinResult.joinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedResponse,
            followerJoinResult.joinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );

        // Join with old member id will not fail because the member id is not updated because of persistence failure
        assertNotEquals(rebalanceResult.followerId, followerJoinResult.joinFuture.get().memberId());
        followerJoinResult = context.sendClassicGroupJoin(request.setMemberId(rebalanceResult.followerId));

        assertTrue(followerJoinResult.records.isEmpty());

        // Join with leader and complete join phase.
        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(
            request.setGroupInstanceId("leader-instance-id")
                .setMemberId(rebalanceResult.leaderId)
        );

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());

        // Sync with leader and receive assignment.
        SyncGroupRequestData syncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withGenerationId(rebalanceResult.generationId + 1)
            .build();

        GroupMetadataManagerTestContext.SyncResult leaderSyncResult = context.sendClassicGroupSync(syncRequest);

        // Simulate a successful write to the log. This will update the group with the new (empty) assignment.
        leaderSyncResult.appendFuture.complete(null);

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            leaderSyncResult.records
        );

        assertTrue(leaderSyncResult.syncFuture.isDone());
        assertTrue(group.isInState(STABLE));
        assertEquals(Errors.NONE.code(), leaderSyncResult.syncFuture.get().errorCode());

        // Sync with old member id will also not fail as the member id is not updated due to persistence failure
        GroupMetadataManagerTestContext.SyncResult oldMemberSyncResult = context.sendClassicGroupSync(
            syncRequest
                .setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId)
        );
        assertTrue(oldMemberSyncResult.records.isEmpty());
        assertTrue(oldMemberSyncResult.syncFuture.isDone());
        assertTrue(group.isInState(STABLE));
        assertEquals(Errors.NONE.code(), oldMemberSyncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinWithUnknownMemberIdAndChangeOfProtocolWhileSelectProtocolUnchanged() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // A static follower rejoin with protocol changing to leader protocol subset won't trigger rebalance if updated
        // group's selectProtocol remain unchanged.
        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols(group.selectProtocol());

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocols(protocols)
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            followerJoinResult.records
        );

        // Simulate a successful write to the log.
        followerJoinResult.appendFuture.complete(null);
        assertTrue(followerJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId)
            .setMemberId(followerJoinResult.joinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedResponse,
            followerJoinResult.joinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );

        // Join with old member id will fail because the member id is updated
        String newFollowerId = followerJoinResult.joinFuture.get().memberId();
        assertNotEquals(rebalanceResult.followerId, newFollowerId);
        followerJoinResult = context.sendClassicGroupJoin(request.setMemberId(rebalanceResult.followerId));

        assertTrue(followerJoinResult.records.isEmpty());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), followerJoinResult.joinFuture.get().errorCode());

        // Sync with old member id will fail because the member id is updated
        SyncGroupRequestData syncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withGenerationId(rebalanceResult.generationId)
            .withMemberId(rebalanceResult.followerId)
            .withAssignment(Collections.emptyList())
            .build();

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(syncRequest);

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), syncResult.syncFuture.get().errorCode());

        // Sync with new member id succeeds
        syncResult = context.sendClassicGroupSync(syncRequest.setMemberId(newFollowerId));

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(rebalanceResult.followerAssignment, syncResult.syncFuture.get().assignment());
    }

    @Test
    public void testStaticMemberRejoinWithKnownLeaderIdToTriggerRebalanceAndFollowerWithChangeofProtocol()
        throws Exception {

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // A static leader rejoin with known member id will trigger rebalance.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertTrue(leaderJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());

        // Rebalance completes immediately after follower rejoins.
        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(
            request.setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId),
            true,
            true
        );

        assertTrue(followerJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.generationId());

        // Leader should get the same assignment as last round.
        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1) // The group has promoted to the new generation.
            .setMemberId(leaderJoinResult.joinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id", "follower-instance-id")
        );

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1) // The group has promoted to the new generation.
            .setMemberId(followerJoinResult.joinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );

        // The follower protocol changed from protocolSuperset to general protocols.
        JoinGroupRequestProtocolCollection protocols = GroupMetadataManagerTestContext.toProtocols("range");

        followerJoinResult = context.sendClassicGroupJoin(
            request.setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId)
                .setProtocols(protocols),
            true,
            true
        );

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());
        // The group will transition to PreparingRebalance due to protocol change from follower.
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by session timeout to kick leader out and complete join phase.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(5000);
        // Both leader and follower heartbeat timers may expire. However, the follower heartbeat expiration
        // will not kick the follower out because it is awaiting a join response.
        assertTrue(timeouts.size() <= 2);
        assertTrue(followerJoinResult.joinFuture.isDone());

        String newFollowerId = followerJoinResult.joinFuture.get().memberId();
        expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 2) // The group has promoted to the new generation.
            .setMemberId(newFollowerId)
            .setLeader(newFollowerId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Set.of("follower-instance-id")
        );
    }

    @Test
    public void testStaticMemberRejoinAsFollowerWithUnknownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // A static follower rejoin with no protocol change will not trigger rebalance.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            followerJoinResult.records
        );
        // Simulate a successful write to log.
        followerJoinResult.appendFuture.complete(null);

        assertTrue(followerJoinResult.joinFuture.isDone());

        // Old leader shouldn't be timed out.
        assertTrue(group.hasStaticMember("leader-instance-id"));

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId) // The group has not changed.
            .setMemberId(followerJoinResult.joinFuture.get().memberId())
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinResult.joinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );
        assertNotEquals(rebalanceResult.followerId, followerJoinResult.joinFuture.get().memberId());

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("follower-instance-id")
                .withGenerationId(rebalanceResult.generationId)
                .withMemberId(followerJoinResult.joinFuture.get().memberId())
                .build()
        );

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(rebalanceResult.followerAssignment, syncResult.syncFuture.get().assignment());
    }

    @Test
    public void testStaticMemberRejoinAsFollowerWithKnownMemberIdAndNoProtocolChange() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // A static follower rejoin with no protocol change will not trigger rebalance.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(rebalanceResult.followerId)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        // No records to write because no metadata changed.
        assertTrue(followerJoinResult.records.isEmpty());
        assertTrue(followerJoinResult.joinFuture.isDone());

        // Old leader shouldn't be timed out.
        assertTrue(group.hasStaticMember("leader-instance-id"));

        JoinGroupResponseData expectedFollowerResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId) // The group has not changed.
            .setMemberId(rebalanceResult.followerId)
            .setLeader(rebalanceResult.leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedFollowerResponse,
            followerJoinResult.joinFuture.get(),
            group,
            STABLE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberRejoinAsFollowerWithMismatchedInstanceId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(rebalanceResult.followerId)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertTrue(followerJoinResult.records.isEmpty());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), followerJoinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberRejoinAsLeaderWithMismatchedInstanceId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), leaderJoinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberSyncAsLeaderWithInvalidMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        SyncGroupRequestData request = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId("invalid-member-id")
            .build();

        GroupMetadataManagerTestContext.SyncResult leaderSyncResult = context.sendClassicGroupSync(request);

        assertTrue(leaderSyncResult.records.isEmpty());
        assertTrue(leaderSyncResult.syncFuture.isDone());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), leaderSyncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testGetDifferentStaticMemberIdAfterEachRejoin() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        String lastMemberId = rebalanceResult.leaderId;
        for (int i = 0; i < 5; i++) {
            JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("leader-instance-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocolSuperset()
                .build();

            GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(
                request,
                true,
                true
            );

            assertEquals(
                List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
                leaderJoinResult.records
            );
            // Simulate a successful write to log.
            leaderJoinResult.appendFuture.complete(null);
            assertTrue(leaderJoinResult.joinFuture.isDone());
            assertEquals(group.staticMemberId("leader-instance-id"), leaderJoinResult.joinFuture.get().memberId());
            assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
            assertNotEquals(lastMemberId, leaderJoinResult.joinFuture.get().memberId());

            lastMemberId = leaderJoinResult.joinFuture.get().memberId();
        }
    }

    @Test
    public void testStaticMemberJoinWithUnknownInstanceIdAndKnownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("unknown-instance-id")
            .withMemberId(rebalanceResult.leaderId)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberReJoinWithIllegalStateAsUnknownMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("follower-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        // Illegal state exception shall trigger since follower id resides in pending member bucket.
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> context.sendClassicGroupJoin(
            request,
            true,
            true
        ));

        String message = exception.getMessage();
        assertTrue(message.contains(group.groupId()));
        assertTrue(message.contains("follower-instance-id"));
    }

    @Test
    public void testStaticMemberFollowerFailToRejoinBeforeRebalanceTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Increase session timeout so that the follower won't be evicted when rebalance timeout is reached.
        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            10000,
            15000
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        String newMemberInstanceId = "new-member-instance-id";
        String leaderId = rebalanceResult.leaderId;

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId(newMemberInstanceId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult newMemberJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertTrue(newMemberJoinResult.records.isEmpty());
        assertFalse(newMemberJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(
            request
                .setGroupInstanceId("leader-instance-id")
                .setMemberId(leaderId),
            true,
            true
        );

        assertTrue(leaderJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());

        // Advance clock by rebalance timeout to complete join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(10000));

        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(newMemberJoinResult.joinFuture.isDone());

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(leaderId)
            .setLeader(leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            leaderJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id", "follower-instance-id", newMemberInstanceId)
        );

        JoinGroupResponseData expectedNewMemberResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(newMemberJoinResult.joinFuture.get().memberId())
            .setLeader(leaderId)
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedNewMemberResponse,
            newMemberJoinResult.joinFuture.get(),
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testStaticMemberLeaderFailToRejoinBeforeRebalanceTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Increase session timeout so that the leader won't be evicted when rebalance timeout is reached.
        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            10000,
            15000
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        String newMemberInstanceId = "new-member-instance-id";
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId(newMemberInstanceId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult newMemberJoinResult = context.sendClassicGroupJoin(
            request,
            true,
            true
        );

        assertTrue(newMemberJoinResult.records.isEmpty());
        assertFalse(newMemberJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        GroupMetadataManagerTestContext.JoinResult oldFollowerJoinResult = context.sendClassicGroupJoin(
            request
                .setGroupInstanceId("follower-instance-id")
                .setMemberId(rebalanceResult.followerId),
            true,
            true
        );

        assertTrue(oldFollowerJoinResult.records.isEmpty());
        assertFalse(oldFollowerJoinResult.joinFuture.isDone());

        // Advance clock by rebalance timeout to complete join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(10000));

        assertTrue(oldFollowerJoinResult.joinFuture.isDone());
        assertTrue(newMemberJoinResult.joinFuture.isDone());

        JoinGroupResponseData oldFollowerJoinResponse = oldFollowerJoinResult.joinFuture.get();
        JoinGroupResponseData newMemberJoinResponse = newMemberJoinResult.joinFuture.get();

        JoinGroupResponseData newLeaderResponse = oldFollowerJoinResponse.leader()
            .equals(oldFollowerJoinResponse.memberId()) ? oldFollowerJoinResponse : newMemberJoinResponse;

        JoinGroupResponseData newFollowerResponse = oldFollowerJoinResponse.leader()
            .equals(oldFollowerJoinResponse.memberId()) ? newMemberJoinResponse : oldFollowerJoinResponse;

        JoinGroupResponseData expectedLeaderResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(newLeaderResponse.memberId())
            .setLeader(newLeaderResponse.memberId())
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(toJoinResponseMembers(group));

        checkJoinGroupResponse(
            expectedLeaderResponse,
            newLeaderResponse,
            group,
            COMPLETING_REBALANCE,
            Set.of("leader-instance-id", "follower-instance-id", newMemberInstanceId)
        );

        JoinGroupResponseData expectedNewMemberResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(rebalanceResult.generationId + 1)
            .setMemberId(newFollowerResponse.memberId())
            .setLeader(newLeaderResponse.memberId())
            .setProtocolName("range")
            .setProtocolType("consumer")
            .setSkipAssignment(false)
            .setMembers(Collections.emptyList());

        checkJoinGroupResponse(
            expectedNewMemberResponse,
            newFollowerResponse,
            group,
            COMPLETING_REBALANCE,
            Collections.emptySet()
        );
    }

    @Test
    public void testSyncGroupReturnsAnErrorWhenProtocolTypeIsInconsistent() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.of("protocolType"),
            Optional.empty(),
            Errors.INCONSISTENT_GROUP_PROTOCOL,
            Optional.empty(),
            Optional.empty()
        );
    }

    @Test
    public void testSyncGroupReturnsAnErrorWhenProtocolNameIsInconsistent() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.empty(),
            Optional.of("protocolName"),
            Errors.INCONSISTENT_GROUP_PROTOCOL,
            Optional.empty(),
            Optional.empty()
        );
    }

    @Test
    public void testSyncGroupSucceedWhenProtocolTypeAndNameAreNotProvided() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.empty(),
            Optional.empty(),
            Errors.NONE,
            Optional.of("consumer"),
            Optional.of("range")
        );
    }

    @Test
    public void testSyncGroupSucceedWhenProtocolTypeAndNameAreConsistent() throws Exception {
        testSyncGroupProtocolTypeAndNameWith(
            Optional.of("consumer"),
            Optional.of("range"),
            Errors.NONE,
            Optional.of("consumer"),
            Optional.of("range")
        );
    }

    private void testSyncGroupProtocolTypeAndNameWith(
        Optional<String> protocolType,
        Optional<String> protocolName,
        Errors expectedError,
        Optional<String> expectedProtocolType,
        Optional<String> expectedProtocolName
    ) throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        // JoinGroup(leader) with the Protocol Type of the group
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolSuperset()
            .build();

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(joinRequest);

        assertTrue(leaderJoinResult.records.isEmpty());
        assertFalse(leaderJoinResult.joinFuture.isDone());

        // JoinGroup(follower) with the Protocol Type of the group
        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(joinRequest.setGroupInstanceId("follower-instance-id"));

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());

        // Advance clock by rebalance timeout to complete join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(10000));
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());

        String leaderId = leaderJoinResult.joinFuture.get().memberId();
        String followerId = followerJoinResult.joinFuture.get().memberId();
        int generationId = leaderJoinResult.joinFuture.get().generationId();

        // SyncGroup with the provided Protocol Type and Name
        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment().setMemberId(leaderId));
        SyncGroupRequestData syncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(leaderId)
            .withProtocolType(protocolType.orElse(null))
            .withProtocolName(protocolName.orElse(null))
            .withGenerationId(generationId)
            .withAssignment(assignment)
            .build();

        GroupMetadataManagerTestContext.SyncResult leaderSyncResult = context.sendClassicGroupSync(syncRequest);
        // Simulate a successful write to the log.
        leaderSyncResult.appendFuture.complete(null);

        assertTrue(leaderSyncResult.syncFuture.isDone());
        assertEquals(expectedError.code(), leaderSyncResult.syncFuture.get().errorCode());
        assertEquals(expectedProtocolType.orElse(null), leaderSyncResult.syncFuture.get().protocolType());
        assertEquals(expectedProtocolName.orElse(null), leaderSyncResult.syncFuture.get().protocolName());

        GroupMetadataManagerTestContext.SyncResult followerSyncResult = context.sendClassicGroupSync(syncRequest.setMemberId(followerId));

        assertTrue(followerSyncResult.records.isEmpty());
        assertTrue(followerSyncResult.syncFuture.isDone());
        assertEquals(expectedError.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(expectedProtocolType.orElse(null), followerSyncResult.syncFuture.get().protocolType());
        assertEquals(expectedProtocolName.orElse(null), followerSyncResult.syncFuture.get().protocolName());
    }

    @Test
    public void testSyncGroupFromUnknownGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // SyncGroup with the provided Protocol Type and Name
        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId("member-id")
                .withGenerationId(1)
                .build()
        );

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testSyncGroupFromUnknownMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupResponseData joinResponse = context.joinClassicGroupAndCompleteJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("leader-instance-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build(),
            true,
            true
        );

        String memberId = joinResponse.memberId();
        int generationId = joinResponse.generationId();

        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment().setMemberId(memberId));
        SyncGroupRequestData syncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(memberId)
            .withGenerationId(generationId)
            .withAssignment(assignment)
            .build();

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(syncRequest);

        // Simulate a successful write to log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(assignment.get(0).assignment(), syncResult.syncFuture.get().assignment());

        // Sync with unknown member.
        syncResult = context.sendClassicGroupSync(syncRequest.setMemberId("unknown-member-id"));

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(ClassicGroupMember.EMPTY_ASSIGNMENT, syncResult.syncFuture.get().assignment());
    }

    @Test
    public void testSyncGroupFromIllegalGeneration() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData joinResponse = context.joinClassicGroupAndCompleteJoin(joinRequest, true, true);

        String memberId = joinResponse.memberId();
        int generationId = joinResponse.generationId();

        // Send the sync group with an invalid generation
        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId)
                .withGenerationId(generationId + 1)
                .build()
        );

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.ILLEGAL_GENERATION.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testSyncGroupAsLeaderAppendFailureTransformsError() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        JoinGroupResponseData joinResponse = context.joinClassicGroupAndCompleteJoin(joinRequest, true, true);

        // Send the sync group with an invalid generation
        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(joinResponse.memberId())
                .withGenerationId(1)
                .build()
        );

        assertFalse(syncResult.syncFuture.isDone());

        // Simulate a failed write to the log.
        syncResult.appendFuture.completeExceptionally(new NotLeaderOrFollowerException());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NOT_COORDINATOR.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testJoinGroupFromUnchangedFollowerDoesNotRebalance() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(joinRequest);

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(leaderJoinResponse.memberId()));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());
        assertEquals(leaderJoinResult.joinFuture.get().generationId(), followerJoinResult.joinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinResult.joinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinResult.joinFuture.get().leader());

        int nextGenerationId = leaderJoinResult.joinFuture.get().generationId();
        String followerId = followerJoinResult.joinFuture.get().memberId();

        // This shouldn't cause a rebalance since protocol information hasn't changed
        followerJoinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(followerId));

        assertTrue(followerJoinResult.records.isEmpty());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());
        assertEquals(nextGenerationId, followerJoinResult.joinFuture.get().generationId());
    }

    @Test
    public void testLeaderFailureInSyncGroup() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(joinRequest);

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(leaderJoinResponse.memberId()));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());
        assertEquals(leaderJoinResult.joinFuture.get().generationId(), followerJoinResult.joinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinResult.joinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinResult.joinFuture.get().leader());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        int nextGenerationId = leaderJoinResult.joinFuture.get().generationId();
        String followerId = followerJoinResult.joinFuture.get().memberId();

        // With no leader SyncGroup, the follower's sync request should fail with an error indicating
        // that it should rejoin
        GroupMetadataManagerTestContext.SyncResult followerSyncResult = context.sendClassicGroupSync(new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(followerId)
            .withGenerationId(nextGenerationId)
            .build());

        assertTrue(followerSyncResult.records.isEmpty());
        assertFalse(followerSyncResult.syncFuture.isDone());

        // Advance clock by session timeout to expire leader heartbeat and prepare rebalance.
        // This should complete follower's sync response. The follower's heartbeat expiration will not kick
        // the follower out because it is awaiting sync.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(10000);
        assertTrue(timeouts.size() <= 2);
        timeouts.forEach(timeout -> assertTrue(timeout.result.records().isEmpty()));

        assertTrue(followerSyncResult.syncFuture.isDone());
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(1, group.numMembers());
        assertTrue(group.hasMember(followerId));
        assertTrue(group.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testSyncGroupFollowerAfterLeader() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(UNKNOWN_MEMBER_ID));

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(leaderJoinResponse.memberId()));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());
        assertEquals(leaderJoinResult.joinFuture.get().generationId(), followerJoinResult.joinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinResult.joinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinResult.joinFuture.get().leader());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        int nextGenerationId = leaderJoinResult.joinFuture.get().generationId();
        String followerId = followerJoinResult.joinFuture.get().memberId();
        byte[] leaderAssignment = new byte[]{0};
        byte[] followerAssignment = new byte[]{1};

        // Sync group with leader to get new assignment.
        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(leaderJoinResponse.memberId())
            .setAssignment(leaderAssignment)
        );
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(followerId)
            .setAssignment(followerAssignment)
        );

        SyncGroupRequestData syncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(leaderJoinResponse.memberId())
            .withGenerationId(leaderJoinResponse.generationId())
            .withAssignment(assignment)
            .build();

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            syncRequest.setGenerationId(nextGenerationId)
        );

        // Simulate a successful write to log. This will update the group's assignment with the new assignment.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(leaderAssignment, syncResult.syncFuture.get().assignment());

        // Sync group with follower to get new assignment.
        GroupMetadataManagerTestContext.SyncResult followerSyncResult = context.sendClassicGroupSync(
            syncRequest
                .setMemberId(followerId)
                .setGenerationId(nextGenerationId)
        );

        assertTrue(followerSyncResult.records.isEmpty());
        assertTrue(followerSyncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(followerAssignment, followerSyncResult.syncFuture.get().assignment());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testSyncGroupLeaderAfterFollower() throws Exception {
        // To get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        GroupMetadataManagerTestContext.JoinResult followerJoinResult = context.sendClassicGroupJoin(joinRequest);

        assertTrue(followerJoinResult.records.isEmpty());
        assertFalse(followerJoinResult.joinFuture.isDone());

        GroupMetadataManagerTestContext.JoinResult leaderJoinResult = context.sendClassicGroupJoin(joinRequest.setMemberId(leaderJoinResponse.memberId()));

        assertTrue(leaderJoinResult.records.isEmpty());
        assertTrue(leaderJoinResult.joinFuture.isDone());
        assertTrue(followerJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderJoinResult.joinFuture.get().errorCode());
        assertEquals(Errors.NONE.code(), followerJoinResult.joinFuture.get().errorCode());
        assertEquals(leaderJoinResult.joinFuture.get().generationId(), followerJoinResult.joinFuture.get().generationId());
        assertEquals(leaderJoinResponse.memberId(), leaderJoinResult.joinFuture.get().leader());
        assertEquals(leaderJoinResponse.memberId(), followerJoinResult.joinFuture.get().leader());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        int nextGenerationId = leaderJoinResult.joinFuture.get().generationId();
        String followerId = followerJoinResult.joinFuture.get().memberId();
        byte[] leaderAssignment = new byte[]{0};
        byte[] followerAssignment = new byte[]{1};

        // Sync group with follower to get new assignment.
        SyncGroupRequestData syncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(leaderJoinResponse.memberId())
            .withGenerationId(leaderJoinResponse.generationId())
            .build();

        GroupMetadataManagerTestContext.SyncResult followerSyncResult = context.sendClassicGroupSync(
            syncRequest
                .setMemberId(followerId)
                .setGenerationId(nextGenerationId)
        );

        assertTrue(followerSyncResult.records.isEmpty());
        assertFalse(followerSyncResult.syncFuture.isDone());

        // Sync group with leader to get new assignment.
        List<SyncGroupRequestAssignment> assignment = new ArrayList<>();
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(leaderJoinResponse.memberId())
            .setAssignment(leaderAssignment)
        );
        assignment.add(new SyncGroupRequestAssignment()
            .setMemberId(followerId)
            .setAssignment(followerAssignment)
        );

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            syncRequest
                .setMemberId(leaderJoinResponse.memberId())
                .setGenerationId(nextGenerationId)
                .setAssignments(assignment)
        );

        // Simulate a successful write to log. This will update the group assignment with the new assignment.
        syncResult.appendFuture.complete(null);

        Map<String, byte[]> updatedAssignment = assignment.stream().collect(Collectors.toMap(
            SyncGroupRequestAssignment::memberId, SyncGroupRequestAssignment::assignment
        ));

        assertEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, updatedAssignment, MetadataVersion.latestTesting())),
            syncResult.records
        );

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertEquals(leaderAssignment, syncResult.syncFuture.get().assignment());

        // Follower sync group should also be completed.
        assertEquals(Errors.NONE.code(), followerSyncResult.syncFuture.get().errorCode());
        assertEquals(followerAssignment, followerSyncResult.syncFuture.get().assignment());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testJoinGroupFromUnchangedLeaderShouldRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        // Join group from the leader should force the group to rebalance, which allows the
        // leader to push new assignment when local metadata changes
        GroupMetadataManagerTestContext.JoinResult leaderRejoinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(leaderJoinResponse.memberId())
                .withDefaultProtocolTypeAndProtocols()
                .build()
        );

        assertTrue(leaderRejoinResult.records.isEmpty());
        assertTrue(leaderRejoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), leaderRejoinResult.joinFuture.get().errorCode());
        assertEquals(leaderJoinResponse.generationId() + 1, leaderRejoinResult.joinFuture.get().generationId());
    }

    @Test
    public void testJoinGroupCompletionWhenPendingMemberJoins() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        // Set up a group in with a pending member. The test checks if the pending member joining
        // completes the rebalancing operation
        JoinGroupResponseData pendingMemberResponse = context.setupGroupWithPendingMember(group).pendingMemberResponse;

        // Compete join group for the pending member
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(pendingMemberResponse.memberId())
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(3, group.allMembers().size());
        assertEquals(0, group.numPendingJoinMembers());
    }

    @Test
    public void testJoinGroupCompletionWhenPendingMemberTimesOut() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        // Set up a group in with a pending member. The test checks if the timeout of the pending member will
        // cause the group to return to a CompletingRebalance state.
        context.setupGroupWithPendingMember(group);

        // Advancing clock by > 2500 (session timeout for the third member)
        // and < 5000 (for first and second members). This will force the coordinator to attempt join
        // completion on heartbeat expiration (since we are in PendingRebalance stage).
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(3000));
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.allMembers().size());
        assertEquals(0, group.numPendingJoinMembers());
    }

    @Test
    public void testGenerationIdIncrementsOnRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(leaderJoinResponse.memberId())
                .withDefaultProtocolTypeAndProtocols()
                .build()
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(2, joinResult.joinFuture.get().generationId());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStaticMemberHeartbeatLeaderWithInvalidMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("leader-instance-id")
                .withMemberId(rebalanceResult.leaderId)
                .withGenerationId(rebalanceResult.generationId)
                .build()
        );

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(rebalanceResult.leaderId)
            .setGenerationId(rebalanceResult.generationId);

        HeartbeatResponseData validHeartbeatResponse = context.sendClassicGroupHeartbeat(heartbeatRequest).response();
        assertEquals(Errors.NONE.code(), validHeartbeatResponse.errorCode());

        assertThrows(FencedInstanceIdException.class, () -> context.sendClassicGroupHeartbeat(
            heartbeatRequest
                .setGroupInstanceId("leader-instance-id")
                .setMemberId("invalid-member-id")
        ));
    }

    @Test
    public void testHeartbeatUnknownGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId("member-id")
            .setGenerationId(-1);

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(heartbeatRequest));
    }

    @Test
    public void testHeartbeatDeadGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        group.transitionTo(DEAD);

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId("member-id")
            .setGenerationId(-1);

        assertThrows(CoordinatorNotAvailableException.class, () -> context.sendClassicGroupHeartbeat(heartbeatRequest));
    }

    @Test
    public void testHeartbeatEmptyGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        group.add(new ClassicGroupMember(
            "member-id",
            Optional.empty(),
            "client-id",
            "client-host",
            10000,
            5000,
            "consumer",
            GroupMetadataManagerTestContext.toProtocols("range")
        ));

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId("member-id")
            .setGenerationId(0);

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(heartbeatRequest).response();
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testHeartbeatUnknownMemberExistingGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId("unknown-member-id")
                .setGenerationId(leaderJoinResponse.generationId())
        ));
    }

    @Test
    public void testHeartbeatDuringPreparingRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(joinRequest, true);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());

        String memberId = joinResult.joinFuture.get().memberId();

        context.sendClassicGroupJoin(joinRequest.setMemberId(memberId));

        assertTrue(group.isInState(PREPARING_REBALANCE));

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId(memberId)
                .setGenerationId(0)
        ).response();

        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testHeartbeatDuringCompletingRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupResponseData leaderJoinResponse =
            context.joinClassicGroupAsDynamicMemberAndCompleteJoin(new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build());

        assertEquals(1, leaderJoinResponse.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId(leaderJoinResponse.memberId())
                .setGenerationId(leaderJoinResponse.generationId())
        ).response();

        assertEquals(new HeartbeatResponseData(), heartbeatResponse);
    }

    @Test
    public void testHeartbeatIllegalGeneration() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        assertThrows(IllegalGenerationException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId(leaderJoinResponse.memberId())
                .setGenerationId(leaderJoinResponse.generationId() + 1)
        ));
    }

    @Test
    public void testValidHeartbeat() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId(leaderJoinResponse.memberId())
                .setGenerationId(leaderJoinResponse.generationId())
        ).response();

        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testClassicGroupMemberSessionTimeout() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Advance clock by session timeout to kick member out.
        context.verifySessionExpiration(group, 5000);

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId(leaderJoinResponse.memberId())
                .setGenerationId(leaderJoinResponse.generationId())
        ));
    }

    @Test
    public void testClassicGroupMemberHeartbeatMaintainsSession() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        // Advance clock by 1/2 of session timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2500));

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId());

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(heartbeatRequest).response();
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());

        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2500));

        heartbeatResponse = context.sendClassicGroupHeartbeat(heartbeatRequest).response();
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testClassicGroupMemberSessionTimeoutDuringRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Add a new member. This should trigger a rebalance. The new member has the
        // 'classicGroupNewMemberJoinTimeoutMs` session timeout, so it has a longer expiration than the existing member.
        GroupMetadataManagerTestContext.JoinResult otherJoinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .withRebalanceTimeoutMs(10000)
                .withSessionTimeoutMs(5000)
                .build()
        );

        assertTrue(otherJoinResult.records.isEmpty());
        assertFalse(otherJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Advance clock by 1/2 of session timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2500));

        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId());

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(heartbeatRequest).response();
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());

        // Advance clock by first member's session timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(5000));

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(heartbeatRequest));

        // Advance clock by remaining rebalance timeout to complete join phase.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2500));

        assertTrue(otherJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), otherJoinResult.joinFuture.get().errorCode());
        assertEquals(1, group.numMembers());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));
    }

    @Test
    public void testRebalanceCompletesBeforeMemberJoins() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        // Create a group with a single member
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAndCompleteJoin(joinRequest, true, true);

        String firstMemberId = leaderJoinResponse.memberId();
        int firstGenerationId = leaderJoinResponse.generationId();

        assertEquals(1, firstGenerationId);
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        SyncGroupRequestData syncRequest = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(firstMemberId)
            .withGenerationId(firstGenerationId)
            .build();

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(syncRequest);

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // Add a new dynamic member. This should trigger a rebalance. The new member has the
        // 'classicGroupNewMemberJoinTimeoutMs` session timeout, so it has a longer expiration than the existing member.
        GroupMetadataManagerTestContext.JoinResult secondMemberJoinResult = context.sendClassicGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setGroupInstanceId(null)
                .setSessionTimeoutMs(2500)
        );

        assertTrue(secondMemberJoinResult.records.isEmpty());
        assertFalse(secondMemberJoinResult.joinFuture.isDone());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Send a couple heartbeats to keep the first member alive while the rebalance finishes.
        HeartbeatRequestData firstMemberHeartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(firstMemberId)
            .setGenerationId(firstGenerationId);

        for (int i = 0; i < 2; i++) {
            GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2500));
            HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(firstMemberHeartbeatRequest).response();
            assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());
        }

        // Advance clock by remaining rebalance timeout to complete join phase.
        // The second member will become the leader. However, as the first member is a static member
        // it will not be kicked out.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(8000));

        assertTrue(secondMemberJoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), secondMemberJoinResult.joinFuture.get().errorCode());
        assertEquals(2, group.numMembers());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        String otherMemberId = secondMemberJoinResult.joinFuture.get().memberId();

        syncResult = context.sendClassicGroupSync(
            syncRequest
                .setGroupInstanceId(null)
                .setMemberId(otherMemberId)
                .setGenerationId(2)
        );

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // The unjoined (first) static member should be remained in the group before session timeout.
        assertThrows(IllegalGenerationException.class, () -> context.sendClassicGroupHeartbeat(firstMemberHeartbeatRequest));

        // Now session timeout the unjoined (first) member. Still keeping the new member.
        List<Errors> expectedErrors = List.of(Errors.NONE, Errors.NONE, Errors.REBALANCE_IN_PROGRESS);
        for (Errors expectedError : expectedErrors) {
            GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2000));
            HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(
                firstMemberHeartbeatRequest
                    .setMemberId(otherMemberId)
                    .setGenerationId(2)
            ).response();

            assertEquals(expectedError.code(), heartbeatResponse.errorCode());
        }
        assertEquals(1, group.numMembers());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        GroupMetadataManagerTestContext.JoinResult otherMemberRejoinResult = context.sendClassicGroupJoin(
            joinRequest
                .setMemberId(otherMemberId)
                .setGroupInstanceId(null)
                .setSessionTimeoutMs(2500)
        );

        assertTrue(otherMemberRejoinResult.records.isEmpty());
        assertTrue(otherMemberRejoinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), otherMemberRejoinResult.joinFuture.get().errorCode());
        assertEquals(3, otherMemberRejoinResult.joinFuture.get().generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        GroupMetadataManagerTestContext.SyncResult otherMemberResyncResult = context.sendClassicGroupSync(
            syncRequest
                .setGroupInstanceId(null)
                .setMemberId(otherMemberId)
                .setGenerationId(3)
        );

        // Simulate a successful write to the log.
        otherMemberResyncResult.appendFuture.complete(null);

        assertTrue(otherMemberResyncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), otherMemberResyncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // The joined member should get heart beat response with no error. Let the new member keep
        // heartbeating for a while to verify that no new rebalance is triggered unexpectedly.
        for (int i = 0; i < 20; i++) {
            GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2000));
            HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(
                firstMemberHeartbeatRequest
                    .setMemberId(otherMemberId)
                    .setGenerationId(3)
            ).response();

            assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        }
    }

    @Test
    public void testSyncGroupEmptyAssignment() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId("group-id")
                .setMemberId(leaderJoinResponse.memberId())
                .setGenerationId(leaderJoinResponse.generationId())
        ).response();

        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testSecondMemberPartiallyJoinAndTimeout() throws Exception {
        // Test if the following scenario completes a rebalance correctly: A new member starts a JoinGroup request with
        // an UNKNOWN_MEMBER_ID, attempting to join a stable group. But never initiates the second JoinGroup request with
        // the provided member ID and times out. The test checks if original member remains the sole member in this group,
        // which should remain stable throughout this test.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        // Create a group with a single member
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withGroupInstanceId("leader-instance-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        JoinGroupResponseData leaderJoinResponse = context.joinClassicGroupAndCompleteJoin(joinRequest, true, true);

        String firstMemberId = leaderJoinResponse.memberId();
        int firstGenerationId = leaderJoinResponse.generationId();

        assertEquals(1, firstGenerationId);
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(firstMemberId)
            .withGenerationId(firstGenerationId)
            .build());

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // Add a new dynamic pending member.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            joinRequest
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setGroupInstanceId(null)
                .setSessionTimeoutMs(5000),
            true,
            true
        );

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(1, group.numPendingJoinMembers());
        assertTrue(group.isInState(STABLE));

        // Heartbeat from the leader to maintain session while timing out pending member.
        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(firstMemberId)
            .setGenerationId(firstGenerationId);

        for (int i = 0; i < 2; i++) {
            GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(2500));
            HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(heartbeatRequest).response();
            assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        }

        // At this point the second member should have been removed from pending list (session timeout),
        // and the group should be in Stable state with only the first member in it.
        assertEquals(1, group.numMembers());
        assertTrue(group.hasMember(firstMemberId));
        assertEquals(1, group.generationId());
        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testRebalanceTimesOutWhenSyncRequestIsNotReceived() throws Exception {
        // This test case ensure that the pending sync expiration does kick out all members
        // if they don't send sync requests before the rebalance timeout. The
        // group is in the Empty state in this case.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Advance clock by 1/2 rebalance timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Advance clock by 1/2 rebalance timeout to expire the pending sync. Members should be removed.
        // The group becomes empty, generating an empty group metadata record.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(rebalanceTimeoutMs / 2);
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, CoordinatorRecord> timeout = timeouts.get(0);
        assertEquals(classicGroupSyncKey("group-id"), timeout.key);
        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            timeout.result.records()
        );

        // Simulate a successful write to the log.
        timeout.result.appendFuture().complete(null);

        // Heartbeats fail because none of the members have sent the sync request
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.UNKNOWN_MEMBER_ID));
        assertTrue(group.isInState(EMPTY));
    }

    @Test
    public void testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromFollowers() throws Exception {
        // This test case ensure that the pending sync expiration does kick out the followers
        // if they don't send a sync request before the rebalance timeout. The
        // group is in the PreparingRebalance state in this case.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Advance clock by 1/2 rebalance timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Leader sends a sync group request.
        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId("group-id")
            .withGenerationId(1)
            .withMemberId(joinResponses.get(0).memberId())
            .build());

        // Simulate a successful write to the log.
        syncResult.appendFuture.complete(null);

        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), syncResult.syncFuture.get().errorCode());
        assertTrue(group.isInState(STABLE));

        // Leader should be able to heartbeat
        context.verifyHeartbeat(group.groupId(), joinResponses.get(0), Errors.NONE);

        // Advance clock by 1/2 rebalance timeout to expire the pending sync. Followers should be removed.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(rebalanceTimeoutMs / 2);
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, CoordinatorRecord> timeout = timeouts.get(0);
        assertEquals(classicGroupSyncKey("group-id"), timeout.key);
        assertTrue(timeout.result.records().isEmpty());

        // Leader should be able to heartbeat
        joinResponses.subList(0, 1).forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.REBALANCE_IN_PROGRESS));

        // Heartbeats fail because none of the followers have sent the sync request
        joinResponses.subList(1, 3).forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.UNKNOWN_MEMBER_ID));

        assertTrue(group.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testRebalanceTimesOutWhenSyncRequestIsNotReceivedFromLeaders() throws Exception {
        // This test case ensure that the pending sync expiration does kick out the leader
        // if it does not send a sync request before the rebalance timeout. The
        // group is in the PreparingRebalance state in this case.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // Advance clock by 1/2 rebalance timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Followers send sync group requests.
        List<CompletableFuture<SyncGroupResponseData>> followerSyncFutures = joinResponses.subList(1, 3).stream()
            .map(response -> {
                GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
                    new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                        .withGroupId("group-id")
                        .withGenerationId(1)
                        .withMemberId(response.memberId())
                        .build()
                );

                assertTrue(syncResult.records.isEmpty());
                assertFalse(syncResult.syncFuture.isDone());
                return syncResult.syncFuture;
            }).collect(Collectors.toList());

        // Advance clock by 1/2 rebalance timeout to expire the pending sync. Leader should be kicked out.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(rebalanceTimeoutMs / 2);
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, CoordinatorRecord> timeout = timeouts.get(0);
        assertEquals(classicGroupSyncKey("group-id"), timeout.key);
        assertTrue(timeout.result.records().isEmpty());

        // Follower sync responses should fail.
        followerSyncFutures.forEach(future -> {
            assertTrue(future.isDone());
            try {
                assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), future.get().errorCode());
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        });

        // Leader heartbeat should fail.
        joinResponses.subList(0, 1).forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.UNKNOWN_MEMBER_ID));

        // Follower heartbeats should succeed.
        joinResponses.subList(1, 3).forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.REBALANCE_IN_PROGRESS));

        assertTrue(group.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testRebalanceDoesNotTimeOutWhenAllSyncAreReceived() throws Exception {
        // This test case ensure that the pending sync expiration does not kick any
        // members out when they have all sent their sync requests. Group should be in Stable state.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int rebalanceTimeoutMs = 5000;
        int sessionTimeoutMs = 5000;
        List<JoinGroupResponseData> joinResponses = context.joinWithNMembers("group-id", 3, rebalanceTimeoutMs, sessionTimeoutMs);
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);
        String leaderId = joinResponses.get(0).memberId();

        // Advance clock by 1/2 rebalance timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // Heartbeats to ensure that heartbeating does not interfere with the
        // delayed sync operation.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // All members send sync group requests.
        List<CompletableFuture<SyncGroupResponseData>> syncFutures = joinResponses.stream().map(response -> {
            GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
                new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                    .withGroupId("group-id")
                    .withGenerationId(1)
                    .withMemberId(response.memberId())
                    .build()
            );

            if (response.memberId().equals(leaderId)) {
                assertEquals(
                    List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
                    syncResult.records
                );

                // Simulate a successful write to the log.
                syncResult.appendFuture.complete(null);
            } else {
                assertTrue(syncResult.records.isEmpty());
            }
            assertTrue(syncResult.syncFuture.isDone());
            return syncResult.syncFuture;
        }).collect(Collectors.toList());

        for (CompletableFuture<SyncGroupResponseData> syncFuture : syncFutures) {
            assertEquals(Errors.NONE.code(), syncFuture.get().errorCode());
        }

        // Advance clock by 1/2 rebalance timeout. Pending sync should already have been cancelled.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // All member heartbeats should succeed.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        // Advance clock a bit more
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs / 2));

        // All member heartbeats should succeed.
        joinResponses.forEach(response -> context.verifyHeartbeat(group.groupId(), response, Errors.NONE));

        assertTrue(group.isInState(STABLE));
    }

    @Test
    public void testHeartbeatDuringRebalanceCausesRebalanceInProgress() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        // First start up a group (with a slightly larger timeout to give us time to heartbeat when the rebalance starts)
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .withRebalanceTimeoutMs(10000)
            .withSessionTimeoutMs(5000)
            .build();

        JoinGroupResponseData leaderJoinResponse =
            context.joinClassicGroupAsDynamicMemberAndCompleteJoin(joinRequest);

        assertEquals(1, leaderJoinResponse.generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // Then join with a new consumer to trigger a rebalance
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            joinRequest.setMemberId(UNKNOWN_MEMBER_ID)
        );

        assertTrue(joinResult.records.isEmpty());
        assertFalse(joinResult.joinFuture.isDone());

        // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress.
        HeartbeatRequestData heartbeatRequest = new HeartbeatRequestData()
            .setGroupId("group-id")
            .setMemberId(leaderJoinResponse.memberId())
            .setGenerationId(leaderJoinResponse.generationId());

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(heartbeatRequest).response();
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse.errorCode());
    }

    @Test
    public void testListGroups() {
        String consumerGroupId = "consumer-group-id";
        String classicGroupId = "classic-group-id";
        String shareGroupId = "share-group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withShareGroupAssignor(assignor)
            .withConsumerGroup(new ConsumerGroupBuilder(consumerGroupId, 10))
            .build();

        // Create one classic group record.
        context.replay(GroupMetadataManagerTestContext.newGroupMetadataRecord(
            classicGroupId,
            new GroupMetadataValue()
                .setMembers(Collections.emptyList())
                .setGeneration(2)
                .setLeader(null)
                .setProtocolType("classic")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds()),
            MetadataVersion.latestTesting()));
        // Create one share group record.
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(shareGroupId, 6));
        context.commit();
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(classicGroupId, false);
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(consumerGroupId, new ConsumerGroupMember.Builder(memberId1)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(consumerGroupId, 11));

        // Test list group response without a group state or group type filter.
        Map<String, ListGroupsResponseData.ListedGroup> actualAllGroupMap =
            context.sendListGroups(Collections.emptyList(), Collections.emptyList()).stream()
                .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        Map<String, ListGroupsResponseData.ListedGroup> expectAllGroupMap =
            Stream.of(
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(classicGroup.groupId())
                    .setProtocolType("classic")
                    .setGroupState(EMPTY.toString())
                    .setGroupType(Group.GroupType.CLASSIC.toString()),
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(consumerGroupId)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setGroupState(ConsumerGroup.ConsumerGroupState.EMPTY.toString())
                    .setGroupType(Group.GroupType.CONSUMER.toString()),
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(shareGroupId)
                    .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                    .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                    .setGroupType(Group.GroupType.SHARE.toString())
            ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // List group with case-insensitive ‘empty’.
        actualAllGroupMap =
            context.sendListGroups(List.of("empty"), Collections.emptyList())
                .stream().collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        context.commit();

        // Test list group response to check assigning state in the consumer group.
        actualAllGroupMap = context.sendListGroups(List.of("assigning"), Collections.emptyList()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap =
            Stream.of(
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(consumerGroupId)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                    .setGroupType(Group.GroupType.CONSUMER.toString())
            ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with group state filter and no group type filter.
        actualAllGroupMap = context.sendListGroups(List.of("Empty"), Collections.emptyList()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroup.groupId())
                .setProtocolType("classic")
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with group type filter.
        actualAllGroupMap = context.sendListGroups(Collections.emptyList(), List.of(Group.GroupType.CLASSIC.toString())).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroup.groupId())
                .setProtocolType("classic")
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with group type filter in a different case.
        actualAllGroupMap = context.sendListGroups(Collections.emptyList(), List.of("Consumer")).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(consumerGroupId)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setGroupType(Group.GroupType.CONSUMER.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        actualAllGroupMap = context.sendListGroups(Collections.emptyList(), List.of("Share")).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        actualAllGroupMap = context.sendListGroups(List.of("empty", "Assigning"), Collections.emptyList()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroup.groupId())
                .setProtocolType(Group.GroupType.CLASSIC.toString())
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(consumerGroupId)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setGroupType(Group.GroupType.CONSUMER.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with invalid group type filter .
        actualAllGroupMap = context.sendListGroups(Collections.emptyList(), List.of("Invalid")).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Collections.emptyMap();

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with invalid group state filter and with no group type filter .
        actualAllGroupMap = context.sendListGroups(List.of("Invalid"), Collections.emptyList()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Collections.emptyMap();

        assertEquals(expectAllGroupMap, actualAllGroupMap);
    }

    @Test
    public void testConsumerGroupDescribeNoErrors() {
        List<String> consumerGroupIds = List.of("group-id-1", "group-id-2");
        int epoch = 10;
        String memberId = "member-id";
        String topicName = "topicName";
        ConsumerGroupMember.Builder memberBuilder = new ConsumerGroupMember.Builder(memberId)
            .setSubscribedTopicNames(List.of(topicName))
            .setServerAssignorName("assignorName");

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(consumerGroupIds.get(0), epoch))
            .withConsumerGroup(new ConsumerGroupBuilder(consumerGroupIds.get(1), epoch)
                .withMember(memberBuilder.build()))
            .build();

        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(consumerGroupIds.get(0))
                .setGroupState(ConsumerGroup.ConsumerGroupState.EMPTY.toString())
                .setAssignorName("range"),
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(consumerGroupIds.get(1))
                .setMembers(List.of(
                    memberBuilder.build().asConsumerGroupDescribeMember(
                        new Assignment(Collections.emptyMap()),
                        new MetadataImageBuilder().build().topics()
                    )
                ))
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setAssignorName("assignorName")
        );
        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.sendConsumerGroupDescribe(consumerGroupIds);

        assertEquals(expected, actual);
    }

    @Test
    public void testConsumerGroupDescribeWithErrors() {
        String groupId = "groupId";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.sendConsumerGroupDescribe(List.of(groupId));
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setErrorMessage("Group " + groupId + " not found.");
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            describedGroup
        );

        assertEquals(expected, actual);
    }

    @Test
    public void testConsumerGroupDescribeBeforeAndAfterCommittingOffset() {
        String consumerGroupId = "consumerGroupId";
        int epoch = 10;
        String memberId1 = "memberId1";
        String memberId2 = "memberId2";
        String topicName = "topicName";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId, topicName, 3)
            .build();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        ConsumerGroupMember.Builder memberBuilder1 = new ConsumerGroupMember.Builder(memberId1)
            .setSubscribedTopicNames(List.of(topicName));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(consumerGroupId, memberBuilder1.build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(consumerGroupId, epoch + 1));

        Map<Uuid, Set<Integer>> assignmentMap = Map.of(topicId, Collections.emptySet());

        ConsumerGroupMember.Builder memberBuilder2 = new ConsumerGroupMember.Builder(memberId2);
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(consumerGroupId, memberBuilder2.build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(consumerGroupId, memberId2, assignmentMap));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(consumerGroupId, memberBuilder2.build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(consumerGroupId, epoch + 2));

        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.groupMetadataManager.consumerGroupDescribe(List.of(consumerGroupId), context.lastCommittedOffset);
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(consumerGroupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setErrorMessage("Group " + consumerGroupId + " not found.");
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            describedGroup
        );
        assertEquals(expected, actual);

        // Commit the offset and test again
        context.commit();

        actual = context.groupMetadataManager.consumerGroupDescribe(List.of(consumerGroupId), context.lastCommittedOffset);
        describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(consumerGroupId)
            .setMembers(List.of(
                memberBuilder1.build().asConsumerGroupDescribeMember(new Assignment(Collections.emptyMap()), metadataImage.topics()),
                memberBuilder2.build().asConsumerGroupDescribeMember(new Assignment(assignmentMap), metadataImage.topics())
            ))
            .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
            .setAssignorName("range")
            .setGroupEpoch(epoch + 2);
        expected = List.of(
            describedGroup
        );
        assertEquals(expected, actual);
    }

    @Test
    public void testStreamsGroupDescribeNoErrors() {
        List<String> streamsGroupIds = Arrays.asList("group-id-1", "group-id-2");
        int epoch = 10;
        String memberId = "member-id";
        StreamsGroupMember.Builder memberBuilder = new StreamsGroupMember.Builder(memberId)
            .setClientTags(Collections.singletonMap("clientTag", "clientValue"))
            .setProcessId("processId")
            .setMemberEpoch(epoch)
            .setPreviousMemberEpoch(epoch - 1);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(new StreamsGroupBuilder(streamsGroupIds.get(0), epoch))
            .withStreamsGroup(new StreamsGroupBuilder(streamsGroupIds.get(1), epoch)
                .withMember(memberBuilder.build()))
            .build();

        List<StreamsGroupDescribeResponseData.DescribedGroup> expected = Arrays.asList(
            new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(streamsGroupIds.get(0))
                .setGroupState(StreamsGroupState.EMPTY.toString())
                .setAssignmentEpoch(0),
            new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(streamsGroupIds.get(1))
                .setMembers(Collections.singletonList(
                    memberBuilder.build().asStreamsGroupDescribeMember(
                        TaskAssignmentTestUtil.mkAssignment(Collections.emptyMap())
                    )
                ))
                .setGroupState(StreamsGroupState.NOT_READY.toString())
        );
        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.sendStreamsGroupDescribe(streamsGroupIds);

        assertEquals(expected, actual);
    }

    @Test
    public void testStreamsGroupDescribeWithErrors() {
        String groupId = "groupId";

        MockTaskAssignor assignor = new MockTaskAssignor("mock");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withTaskAssignors(Collections.singletonList(assignor))
            .build();

        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.sendStreamsGroupDescribe(Collections.singletonList(groupId));
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code());
        List<StreamsGroupDescribeResponseData.DescribedGroup> expected = Collections.singletonList(
            describedGroup
        );

        assertEquals(expected, actual);
    }

    @Test
    public void testStreamsGroupDescribeBeforeAndAfterCommittingOffset() {
        String streamsGroupId = "streamsGroupId";
        int epoch = 10;
        String memberId1 = "memberId1";
        String memberId2 = "memberId2";
        String subtopologyId = "subtopology1";

        MockTaskAssignor assignor = new MockTaskAssignor("mock");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withTaskAssignors(Collections.singletonList(assignor))
            .build();

        StreamsGroupMember.Builder memberBuilder1 = new StreamsGroupMember.Builder(memberId1);
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord(streamsGroupId, memberBuilder1.build()));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord(streamsGroupId, epoch + 1));

        Map<String, Set<Integer>> assignmentMap = new HashMap<>();
        assignmentMap.put(subtopologyId, Collections.emptySet());

        StreamsGroupMember.Builder memberBuilder2 = new StreamsGroupMember.Builder(memberId2);
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord(streamsGroupId, memberBuilder2.build()));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord(streamsGroupId, memberId2, assignmentMap, assignmentMap, assignmentMap));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentRecord(streamsGroupId, memberBuilder2.build()));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord(streamsGroupId, epoch + 2));

        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.groupMetadataManager.streamsGroupDescribe(Collections.singletonList(streamsGroupId), context.lastCommittedOffset);
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(streamsGroupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code());
        assertEquals(1, actual.size());
        assertEquals(describedGroup, actual.get(0));

        // Commit the offset and test again
        context.commit();

        actual = context.groupMetadataManager.streamsGroupDescribe(Collections.singletonList(streamsGroupId), context.lastCommittedOffset);
        describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(streamsGroupId)
            .setMembers(Arrays.asList(
                memberBuilder1.build().asStreamsGroupDescribeMember(TaskAssignmentTestUtil.mkAssignment(Collections.emptyMap())),
                memberBuilder2.build().asStreamsGroupDescribeMember(TaskAssignmentTestUtil.mkAssignment(assignmentMap, assignmentMap, assignmentMap))
            ))
            .setGroupState(StreamsGroup.StreamsGroupState.NOT_READY.toString())
            .setGroupEpoch(epoch + 2);
        assertEquals(1, actual.size());
        assertEquals(describedGroup, actual.get(0));
    }
    
    @Test
    public void testDescribeGroupStable() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataValue.MemberMetadata memberMetadata = new GroupMetadataValue.MemberMetadata()
            .setMemberId("member-id")
            .setGroupInstanceId("group-instance-id")
            .setClientHost("client-host")
            .setClientId("client-id")
            .setAssignment(new byte[]{0})
            .setSubscription(new byte[]{0, 1, 2});
        GroupMetadataValue groupMetadataValue = new GroupMetadataValue()
            .setMembers(List.of(memberMetadata))
            .setProtocolType("consumer")
            .setProtocol("range")
            .setCurrentStateTimestamp(context.time.milliseconds());

        context.replay(GroupMetadataManagerTestContext.newGroupMetadataRecord(
            "group-id",
            groupMetadataValue,
            MetadataVersion.latestTesting()
        ));
        context.verifyDescribeGroupsReturnsDeadGroup("group-id");
        context.commit();

        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = List.of(
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setGroupState(STABLE.toString())
                .setProtocolType(groupMetadataValue.protocolType())
                .setProtocolData(groupMetadataValue.protocol())
                .setMembers(List.of(
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId(memberMetadata.memberId())
                        .setGroupInstanceId(memberMetadata.groupInstanceId())
                        .setClientId(memberMetadata.clientId())
                        .setClientHost(memberMetadata.clientHost())
                        .setMemberMetadata(memberMetadata.subscription())
                        .setMemberAssignment(memberMetadata.assignment())
                ))
        );

        List<DescribeGroupsResponseData.DescribedGroup> describedGroups =
            context.describeGroups(List.of("group-id"));

        assertEquals(expectedDescribedGroups, describedGroups);
    }

    @Test
    public void testDescribeGroupRebalancing() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataValue.MemberMetadata memberMetadata = new GroupMetadataValue.MemberMetadata()
            .setMemberId("member-id")
            .setGroupInstanceId("group-instance-id")
            .setClientHost("client-host")
            .setClientId("client-id")
            .setAssignment(new byte[]{0})
            .setSubscription(new byte[]{0, 1, 2});
        GroupMetadataValue groupMetadataValue = new GroupMetadataValue()
            .setMembers(List.of(memberMetadata))
            .setProtocolType("consumer")
            .setProtocol("range")
            .setCurrentStateTimestamp(context.time.milliseconds());

        context.replay(GroupMetadataManagerTestContext.newGroupMetadataRecord(
            "group-id",
            groupMetadataValue,
            MetadataVersion.latestTesting()
        ));
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);
        context.groupMetadataManager.prepareRebalance(group, "trigger rebalance");

        context.verifyDescribeGroupsReturnsDeadGroup("group-id");
        context.commit();

        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = List.of(
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setGroupState(PREPARING_REBALANCE.toString())
                .setProtocolType(groupMetadataValue.protocolType())
                .setProtocolData("")
                .setMembers(List.of(
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId(memberMetadata.memberId())
                        .setGroupInstanceId(memberMetadata.groupInstanceId())
                        .setClientId(memberMetadata.clientId())
                        .setClientHost(memberMetadata.clientHost())
                        .setMemberAssignment(memberMetadata.assignment())
                ))
        );

        List<DescribeGroupsResponseData.DescribedGroup> describedGroups =
            context.describeGroups(List.of("group-id"));

        assertEquals(expectedDescribedGroups, describedGroups);
    }

    @Test
    public void testDescribeGroupsGroupIdNotFoundException() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.verifyDescribeGroupsReturnsDeadGroup("group-id");
    }

    @Test
    public void testDescribeGroupsBeforeV6GroupIdNotFoundException() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.verifyDescribeGroupsBeforeV6ReturnsDeadGroup("group-id");
    }

    @Test
    public void testGroupStuckInRebalanceTimeoutDueToNonjoinedStaticMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        int longSessionTimeoutMs = 10000;
        int rebalanceTimeoutMs = 5000;
        GroupMetadataManagerTestContext.RebalanceResult rebalanceResult = context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id",
            rebalanceTimeoutMs,
            longSessionTimeoutMs
        );
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);

        // New member joins
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocolSuperset()
                .withSessionTimeoutMs(longSessionTimeoutMs)
                .build()
        );

        // The new dynamic member has been elected as leader
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(rebalanceTimeoutMs));
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(joinResult.joinFuture.get().leader(), joinResult.joinFuture.get().memberId());
        assertEquals(3, joinResult.joinFuture.get().members().size());
        assertEquals(2, joinResult.joinFuture.get().generationId());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        assertEquals(
            Set.of(rebalanceResult.leaderId, rebalanceResult.followerId, joinResult.joinFuture.get().memberId()),
            group.allMemberIds()
        );
        assertEquals(
            Set.of(rebalanceResult.leaderId, rebalanceResult.followerId),
            group.allStaticMemberIds()
        );
        assertEquals(
            Set.of(joinResult.joinFuture.get().memberId()),
            group.allDynamicMemberIds()
        );

        // Send a special leave group request from static follower, moving group towards PreparingRebalance
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId(rebalanceResult.followerId)
                        .setGroupInstanceId("follower-instance-id")
                ))
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setMemberId(rebalanceResult.followerId)
                    .setGroupInstanceId("follower-instance-id")));

        assertEquals(expectedResponse, leaveResult.response());
        assertTrue(group.isInState(PREPARING_REBALANCE));

        context.sleep(rebalanceTimeoutMs);
        // Only static leader is maintained, and group is stuck at PreparingRebalance stage
        assertTrue(group.allDynamicMemberIds().isEmpty());
        assertEquals(Collections.singleton(rebalanceResult.leaderId), group.allMemberIds());
        assertTrue(group.allDynamicMemberIds().isEmpty());
        assertEquals(2, group.generationId());
        assertTrue(group.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testPendingMembersLeaveGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");
        JoinGroupResponseData pendingJoinResponse = context.setupGroupWithPendingMember(group).pendingMemberResponse;

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId(pendingJoinResponse.memberId())
                ))
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingJoinResponse.memberId())));

        assertEquals(expectedResponse, leaveResult.response());
        assertTrue(leaveResult.records().isEmpty());

        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(2, group.allMembers().size());
        assertEquals(2, group.allDynamicMemberIds().size());
        assertEquals(0, group.numPendingJoinMembers());
    }

    @Test
    public void testLeaveGroupInvalidGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("invalid-group-id")
        ));
    }

    @Test
    public void testLeaveGroupUnknownGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("unknown-group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId("member-id")
                ))
        ));
    }

    @Test
    public void testLeaveGroupUnknownMemberIdExistingGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        context.joinClassicGroupAsDynamicMemberAndCompleteJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build()
        );

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId("unknown-member-id")
                ))
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId("unknown-member-id")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())));

        assertEquals(expectedResponse, leaveResult.response());
        assertTrue(leaveResult.records().isEmpty());
    }

    @Test
    public void testLeaveDeadGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");
        group.transitionTo(DEAD);

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId("member-id")
                ))
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());

        assertEquals(expectedResponse, leaveResult.response());
        assertTrue(leaveResult.records().isEmpty());
    }

    @Test
    public void testValidLeaveGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        JoinGroupResponseData joinResponse = context.joinClassicGroupAsDynamicMemberAndCompleteJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build()
        );

        // Dynamic member leaves. The group becomes empty.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId(joinResponse.memberId())
                ))
        );
        assertEquals(
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, group.groupAssignment(), MetadataVersion.latestTesting())),
            leaveResult.records()
        );
        // Simulate a successful write to the log.
        leaveResult.appendFuture().complete(null);

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(joinResponse.memberId())));

        assertEquals(expectedResponse, leaveResult.response());
        assertTrue(group.isInState(EMPTY));
        assertEquals(2, group.generationId());
    }

    @Test
    public void testLeaveGroupWithFencedInstanceId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        context.joinClassicGroupAndCompleteJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("group-instance-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build(),
            true,
            true
        );

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setGroupInstanceId("group-instance-id")
                        .setMemberId("other-member-id") // invalid member id
                ))
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("group-instance-id")
                    .setMemberId("other-member-id")
                    .setErrorCode(Errors.FENCED_INSTANCE_ID.code())));

        assertEquals(expectedResponse, leaveResult.response());
        assertTrue(leaveResult.records().isEmpty());
    }

    @Test
    public void testLeaveGroupStaticMemberWithUnknownMemberId() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        context.joinClassicGroupAndCompleteJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withGroupInstanceId("group-instance-id")
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withDefaultProtocolTypeAndProtocols()
                .build(),
            true,
            true
        );

        // Having unknown member id will not affect the request processing due to valid group instance id.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setGroupInstanceId("group-instance-id")
                        .setMemberId(UNKNOWN_MEMBER_ID)
                ))
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("group-instance-id")));

        assertEquals(expectedResponse, leaveResult.response());
    }

    @Test
    public void testStaticMembersValidBatchLeaveGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(
                    List.of(
                        new MemberIdentity()
                            .setGroupInstanceId("leader-instance-id"),
                        new MemberIdentity()
                            .setGroupInstanceId("follower-instance-id")
                    )
                )
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("leader-instance-id"),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("follower-instance-id")));

        assertEquals(expectedResponse, leaveResult.response());
    }

    @Test
    public void testStaticMembersLeaveUnknownGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("invalid-group-id") // Invalid group id
                .setMembers(
                    List.of(
                        new MemberIdentity()
                            .setGroupInstanceId("leader-instance-id"),
                        new MemberIdentity()
                            .setGroupInstanceId("follower-instance-id")
                    )
                )
        ));
    }

    @Test
    public void testStaticMembersFencedInstanceBatchLeaveGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(
                    List.of(
                        new MemberIdentity()
                            .setGroupInstanceId("leader-instance-id"),
                        new MemberIdentity()
                            .setGroupInstanceId("follower-instance-id")
                            .setMemberId("invalid-member-id")
                    )
                )
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("leader-instance-id"),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("follower-instance-id")
                    .setMemberId("invalid-member-id")
                    .setErrorCode(Errors.FENCED_INSTANCE_ID.code())));

        assertEquals(expectedResponse, leaveResult.response());
    }

    @Test
    public void testStaticMembersUnknownInstanceBatchLeaveGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.staticMembersJoinAndRebalance(
            "group-id",
            "leader-instance-id",
            "follower-instance-id"
        );

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(
                    List.of(
                        new MemberIdentity()
                            .setGroupInstanceId("unknown-instance-id"), // Unknown instance id
                        new MemberIdentity()
                            .setGroupInstanceId("follower-instance-id")
                    )
                )
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("unknown-instance-id")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("follower-instance-id")));

        assertEquals(expectedResponse, leaveResult.response());
        assertTrue(leaveResult.records().isEmpty());
    }

    @Test
    public void testPendingMemberBatchLeaveGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");
        JoinGroupResponseData pendingJoinResponse = context.setupGroupWithPendingMember(group).pendingMemberResponse;

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(
                    List.of(
                        new MemberIdentity()
                            .setGroupInstanceId("unknown-instance-id"), // Unknown instance id
                        new MemberIdentity()
                            .setMemberId(pendingJoinResponse.memberId())
                    )
                )
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId("unknown-instance-id")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingJoinResponse.memberId())));

        assertEquals(expectedResponse, leaveResult.response());
    }

    @Test
    public void testJoinedMemberPendingMemberBatchLeaveGroup() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");
        GroupMetadataManagerTestContext.PendingMemberGroupResult pendingMemberGroupResult = context.setupGroupWithPendingMember(group);

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(
                    List.of(
                        new MemberIdentity()
                            .setMemberId(pendingMemberGroupResult.leaderId),
                        new MemberIdentity()
                            .setMemberId(pendingMemberGroupResult.followerId),
                        new MemberIdentity()
                            .setMemberId(pendingMemberGroupResult.pendingMemberResponse.memberId())
                    )
                )
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingMemberGroupResult.leaderId),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingMemberGroupResult.followerId),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingMemberGroupResult.pendingMemberResponse.memberId())));

        assertEquals(expectedResponse, leaveResult.response());
    }

    @Test
    public void testJoinedMemberPendingMemberBatchLeaveGroupWithUnknownMember() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");
        GroupMetadataManagerTestContext.PendingMemberGroupResult pendingMemberGroupResult = context.setupGroupWithPendingMember(group);

        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(
                    List.of(
                        new MemberIdentity()
                            .setMemberId(pendingMemberGroupResult.leaderId),
                        new MemberIdentity()
                            .setMemberId(pendingMemberGroupResult.followerId),
                        new MemberIdentity()
                            .setMemberId(pendingMemberGroupResult.pendingMemberResponse.memberId()),
                        new MemberIdentity()
                            .setMemberId("unknown-member-id")
                    )
                )
        );

        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setMembers(List.of(
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingMemberGroupResult.leaderId),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingMemberGroupResult.followerId),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId(pendingMemberGroupResult.pendingMemberResponse.memberId()),
                new LeaveGroupResponseData.MemberResponse()
                    .setGroupInstanceId(null)
                    .setMemberId("unknown-member-id")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())));

        assertEquals(expectedResponse, leaveResult.response());
    }

    @Test
    public void testClassicGroupDelete() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.createClassicGroup("group-id");

        List<CoordinatorRecord> expectedRecords = List.of(GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id"));
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.createGroupTombstoneRecords("group-id", records);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void testClassicGroupMaybeDelete() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        List<CoordinatorRecord> expectedRecords = List.of(GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id"));
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.maybeDeleteGroup("group-id", records);
        assertEquals(expectedRecords, records);

        records = new ArrayList<>();
        group.transitionTo(PREPARING_REBALANCE);
        context.groupMetadataManager.maybeDeleteGroup("group-id", records);
        assertEquals(Collections.emptyList(), records);

        records = new ArrayList<>();
        context.groupMetadataManager.maybeDeleteGroup("invalid-group-id", records);
        assertEquals(Collections.emptyList(), records);
    }

    @Test
    public void testConsumerGroupDelete() {
        String groupId = "group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.createGroupTombstoneRecords("group-id", records);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void testConsumerGroupMaybeDelete() {
        String groupId = "group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.maybeDeleteGroup(groupId, records);
        assertEquals(expectedRecords, records);

        records = new ArrayList<>();
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()));
        context.groupMetadataManager.maybeDeleteGroup(groupId, records);
        assertEquals(Collections.emptyList(), records);
    }

    @Test
    public void testClassicGroupCompletedRebalanceSensor() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.joinClassicGroupAsDynamicMemberAndCompleteRebalance("group-id");
        verify(context.metrics).record(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME);
    }

    @Test
    public void testConsumerGroupRebalanceSensor() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        verify(context.metrics).record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
    }

    @Test
    public void testOnClassicGroupStateTransitionOnLoading() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ClassicGroup group = new ClassicGroup(
            new LogContext(),
            "group-id",
            EMPTY,
            context.time
        );

        // Even if there are more group metadata records loaded than tombstone records, the last replayed record
        // (tombstone in this test) is the latest state of the group. Hence, the overall metric count should be 0.
        IntStream.range(0, 5).forEach(__ ->
            context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, Collections.emptyMap(), MetadataVersion.LATEST_PRODUCTION))
        );
        IntStream.range(0, 4).forEach(__ ->
            context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id"))
        );
    }

    @Test
    public void testOnConsumerGroupStateTransition() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Replaying a consumer group epoch record should increment metric.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("group-id", 1));
        verify(context.metrics, times(1)).onConsumerGroupStateTransition(null, ConsumerGroup.ConsumerGroupState.EMPTY);

        // Replaying a consumer group epoch record for a group that has already been created should not increment metric.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("group-id", 1));
        verify(context.metrics, times(1)).onConsumerGroupStateTransition(null, ConsumerGroup.ConsumerGroupState.EMPTY);

        // Creating and replaying tombstones for a group should remove group and decrement metric.
        List<CoordinatorRecord> tombstones = new ArrayList<>();
        Group group = context.groupMetadataManager.group("group-id");
        group.createGroupTombstoneRecords(tombstones);
        tombstones.forEach(context::replay);
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.group("group-id"));
        verify(context.metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.EMPTY, null);

        // Replaying a tombstone for a group that has already been removed should not decrement metric.
        tombstones.forEach(context::replay);
        verify(context.metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.EMPTY, null);
    }

    @Test
    public void testOnConsumerGroupStateTransitionOnLoading() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Even if there are more group epoch records loaded than tombstone records, the last replayed record
        // (tombstone in this test) is the latest state of the group. Hence, the overall metric count should be 0.
        IntStream.range(0, 5).forEach(__ ->
            context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("group-id", 0))
        );
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord("group-id"));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord("group-id"));
        IntStream.range(0, 3).forEach(__ -> {
            context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord("group-id"));
            context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord("group-id"));
        });

        verify(context.metrics, times(1)).onConsumerGroupStateTransition(null, ConsumerGroup.ConsumerGroupState.EMPTY);
        verify(context.metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.EMPTY, null);
    }

    @Test
    public void testConsumerGroupHeartbeatWithNonEmptyClassicGroup() {
        String classicGroupId = "classic-group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .build();
        ClassicGroup classicGroup = new ClassicGroup(
            new LogContext(),
            classicGroupId,
            EMPTY,
            context.time
        );
        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(classicGroup, classicGroup.groupAssignment(), MetadataVersion.latestTesting()));

        context.groupMetadataManager.getOrMaybeCreateClassicGroup(classicGroupId, false).transitionTo(PREPARING_REBALANCE);
        assertThrows(GroupIdNotFoundException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(classicGroupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setServerAssignor(NoOpPartitionAssignor.NAME)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupHeartbeatWithEmptyClassicGroup() {
        String classicGroupId = "classic-group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .build();
        ClassicGroup classicGroup = new ClassicGroup(
            new LogContext(),
            classicGroupId,
            EMPTY,
            context.time
        );
        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(classicGroup, classicGroup.groupAssignment(), MetadataVersion.latestTesting()));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(classicGroupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setAssignedPartitions(Collections.emptyMap())
            .build();

        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(classicGroupId),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(classicGroupId, expectedMember),
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(classicGroupId, 1),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(classicGroupId, memberId, Collections.emptyMap()),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(classicGroupId, 1),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(classicGroupId, expectedMember)
            ),
            result.records()
        );
        assertEquals(
            Group.GroupType.CONSUMER,
            context.groupMetadataManager.consumerGroup(classicGroupId).type()
        );
    }

    @Test
    public void testClassicGroupJoinWithEmptyConsumerGroup() throws Exception {
        String consumerGroupId = "consumer-group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(consumerGroupId, 10))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(consumerGroupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true);

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(consumerGroupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(consumerGroupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(consumerGroupId)
        );

        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(expectedRecords, joinResult.records.subList(0, expectedRecords.size()));
        assertEquals(
            Group.GroupType.CLASSIC,
            context.groupMetadataManager.getOrMaybeCreateClassicGroup(consumerGroupId, false).type()
        );
    }

    @Test
    public void testConsumerGroupHeartbeatWithStableClassicGroup() {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            ))
        )));

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(
                    new TopicPartition(fooTopicName, 0),
                    new TopicPartition(barTopicName, 0)
                )
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId1,
            Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(barTopicName, 0)
            ))))
        );

        // Create a stable classic group with member 1.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols,
                assignments.get(memberId1)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments, metadataImage.features().metadataVersion()));
        context.commit();
        group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);

        // A new member 2 with new protocol joins the classic group, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(Collections.emptyList()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(barTopicId, 0)))
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName("range")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Collections.emptyMap())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, expectedMember1.assignedPartitions()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),

            // Member 2 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            // The subscription metadata hasn't been updated during the conversion, so a new one is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 1),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
            )),

            // Newly joining member 2 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, assignor.targetPartitions(memberId2)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, assignor.targetPartitions(memberId1)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 1),

            // Member 2 has no pending revoking partition. Bump its member epoch and transition to UNRELEASED_PARTITIONS.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result.records());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, 45000);

        // Simulate a failed replay. The context is rolled back and the group is converted back to the classic group.
        context.rollback();
        assertEquals(group, context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false));
    }

    @Test
    public void testConsumerGroupHeartbeatWithPreparingRebalanceClassicGroup() throws Exception {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        String memberId3 = "member-id-3";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols1 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols1.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(
                    new TopicPartition(fooTopicName, 0),
                    new TopicPartition(fooTopicName, 1)
                )
            ))))
        );

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols2 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols2.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(new TopicPartition(barTopicName, 0))
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId1, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1)
            )))),
            memberId2, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(barTopicName, 0)
            ))))
        );

        // Construct a stable group with two members.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols1,
                assignments.get(memberId1)
            )
        );
        group.add(
            new ClassicGroupMember(
                memberId2,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols2,
                assignments.get(memberId2)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments, metadataImage.features().metadataVersion()));
        context.commit();
        group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);

        // The leader rejoins, triggering a rebalance.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId1)
                .withProtocols(protocols1)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Another new member 3 joins with new protocol, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeatResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(Collections.emptyList()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols1))
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)))
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols2))
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(barTopicId, 0)))
            .build();

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName("range")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Collections.emptyMap())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1 and member 2.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, expectedMember1.assignedPartitions()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, expectedMember2.assignedPartitions()),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 0),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2),

            // Member 3 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),

            // The subscription metadata hasn't been updated during the conversion, so a new one is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
            )),

            // Newly joining member 3 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, assignor.targetPartitions(memberId1)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, assignor.targetPartitions(memberId3)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 1),

            // Member 3 has no pending revoking partition. Bump its member epoch and transition to UNRELEASED_PARTITIONS.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords, consumerGroupHeartbeatResult.records());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), joinResult.joinFuture.get().errorCode());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, expectedMember2.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId3, 45000);

        // Simulate a failed replay. The context is rolled back and the group is converted back to the classic group.
        context.rollback();
        assertEquals(group, context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false));
    }

    @Test
    public void testConsumerGroupHeartbeatToClassicGroupFromExistingStaticMember() {
        String groupId = "group-id";
        String memberId = "member-id";
        String instanceId = "instance-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addRacks()
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName(NoOpPartitionAssignor.NAME)
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName),
                null,
                List.of(new TopicPartition(fooTopicName, 0))
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId,
            Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(
                List.of(new TopicPartition(fooTopicName, 0))
            )))
        );

        // Create a stable classic group with a static member.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of(NoOpPartitionAssignor.NAME));
        group.add(
            new ClassicGroupMember(
                memberId,
                Optional.of(instanceId),
                DEFAULT_CLIENT_ID,
                DEFAULT_CLIENT_ADDRESS.toString(),
                10000,
                5000,
                "consumer",
                protocols,
                assignments.get(memberId)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments, metadataImage.features().metadataVersion()));
        context.commit();

        // The static member rejoins with new protocol after a restart, triggering the upgrade.
        String newMemberId = Uuid.randomUuid().toString();
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(newMemberId)
                .setInstanceId(instanceId)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setSubscribedTopicNames(List.of(fooTopicName))
                .setTopicPartitions(Collections.emptyList()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );

        ConsumerGroupMember expectedClassicMember = new ConsumerGroupMember.Builder(memberId)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)))
            .build();

        // The memberId is generated by the consumer and should be retained
        // for the entire lifetime of the process until termination.
        assertEquals(
            newMemberId,
            result.response().memberId(),
            "Server should not generate a new memberId since the consumer has already generated its own."
        );

        ConsumerGroupMember expectedReplacingConsumerMember = new ConsumerGroupMember.Builder(newMemberId)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setClientId(expectedClassicMember.clientId())
            .setClientHost(expectedClassicMember.clientHost())
            .setSubscribedTopicNames(new ArrayList<>(expectedClassicMember.subscribedTopicNames()))
            .setRebalanceTimeoutMs(expectedClassicMember.rebalanceTimeoutMs())
            .setAssignedPartitions(expectedClassicMember.assignedPartitions())
            .setClassicMemberMetadata(expectedClassicMember.classicMemberMetadata().get())
            .build();

        ConsumerGroupMember expectedFinalConsumerMember = new ConsumerGroupMember.Builder(expectedReplacingConsumerMember)
            .setMemberEpoch(1)
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(5000)
            .setClassicMemberMetadata(null)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with the static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedClassicMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, expectedClassicMember.assignedPartitions()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedClassicMember),

            // Remove the static member because the rejoining member replaces it.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

            // Create the new static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedReplacingConsumerMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, mkAssignment(mkTopicAssignment(fooTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedReplacingConsumerMember),

            // The static member rejoins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedFinalConsumerMember),

            // The subscription metadata hasn't been updated during the conversion, so a new one is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 1))),

            // Newly joining static member bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, mkAssignment(mkTopicAssignment(fooTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 1),

            // The newly created static member takes the assignment from the existing member.
            // Bump its member epoch and transition to STABLE.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedFinalConsumerMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
        context.assertSessionTimeout(groupId, newMemberId, 45000);
    }

    @Test
    public void testConsumerGroupHeartbeatToClassicGroupWithEmptyAssignmentMember() throws ExecutionException, InterruptedException {
        String groupId = "group-id";
        String memberId2 = "member-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName)
            ))))
        );

        // Member 1 joins, creating a new classic group.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocols(protocols)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );

        // Triggering completion of the rebalance.
        // Member 1 has never synced so its assignment is empty.
        context.sleep(3000 + 1);
        String memberId1 = joinResult.joinFuture.get().memberId();
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // A new member 2 with new protocol joins the classic group, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(Collections.emptyList()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
            )
            .setAssignedPartitions(Collections.emptyMap())
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Collections.emptyMap())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, expectedMember1.assignedPartitions()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),

            // Member 2 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            // The subscription metadata hasn't been updated during the conversion, so a new one is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 1),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
            )),

            // Newly joining member 2 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, Collections.emptyMap()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 2),

            // Member 2 has no pending revoking partition or pending release partition.
            // Bump its member epoch and transition to STABLE.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result.records());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, 45000);
    }

    @Test
    public void testConsumerGroupHeartbeatFromExistingClassicStaticMember() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String instanceId1 = "instance-id-1";
        String instanceId2 = "instance-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setInstanceId(instanceId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId(instanceId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        // Consumer group with two static members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
            fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
            barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
        )));

        context.commit();

        // The member 1 with the classic protocol upgrades, heartbeating with new protocol.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setInstanceId(instanceId1)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setSubscribedTopicNames(new ArrayList<>(member1.subscribedTopicNames()))
                .setTopicPartitions(Collections.emptyList()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );


        // The memberId is generated by the consumer itself, the consumer should retain this memberId
        // for its entire lifetime until the process terminates.
        assertEquals(
            memberId1,
            result.response().memberId(),
            "Server should not generate a new memberId since the consumer has already generated its own."
        );

        ConsumerGroupMember expectedReplacingConsumerMember = new ConsumerGroupMember.Builder(memberId1)
            .setInstanceId(instanceId1)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setClientId(member1.clientId())
            .setClientHost(member1.clientHost())
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setSubscribedTopicNames(new ArrayList<>(member1.subscribedTopicNames()))
            .setRebalanceTimeoutMs(member1.rebalanceTimeoutMs())
            .setAssignedPartitions(member1.assignedPartitions())
            .setClassicMemberMetadata(member1.classicMemberMetadata().get())
            .build();

        ConsumerGroupMember expectedFinalConsumerMember = new ConsumerGroupMember.Builder(expectedReplacingConsumerMember)
            .setMemberEpoch(10)
            .setRebalanceTimeoutMs(5000)
            .setClassicMemberMetadata(null)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove the existing static member 1 because the rejoining member replaces it.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),

            // Create the new static member 1.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedReplacingConsumerMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, member1.assignedPartitions()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedReplacingConsumerMember),

            // The static member rejoins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedFinalConsumerMember),

            // The newly created static member 1 takes the assignment from the existing member 1.
            // Bump its member epoch and transition to STABLE.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedFinalConsumerMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
        context.assertSessionTimeout(groupId, memberId1, 45000);
    }

    @Test
    public void testConsumerGroupHeartbeatWithCompletingRebalanceClassicGroup() throws Exception {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        String memberId3 = "member-id-3";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols1 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols1.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(
                    new TopicPartition(fooTopicName, 0),
                    new TopicPartition(fooTopicName, 1)
                )
            ))))
        );

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols2 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols2.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(new TopicPartition(barTopicName, 0))
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId1, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1)
            )))),
            memberId2, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(barTopicName, 0)
            ))))
        );

        // Construct a stable group with two members.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols1,
                assignments.get(memberId1)
            )
        );
        group.add(
            new ClassicGroupMember(
                memberId2,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols2,
                assignments.get(memberId2)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments, metadataImage.features().metadataVersion()));
        context.commit();
        group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);

        // The leader rejoins, triggering a rebalance.
        context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId1)
                .withProtocols(protocols1)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );

        // The follower rejoins. All members have rejoined so the group transitions to COMPLETING_REBALANCE state.
        context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId2)
                .withProtocols(protocols2)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId2)
                .withGenerationId(1)
                .build());

        // Another new member 3 joins with new protocol, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeatResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(Collections.emptyList()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols1))
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)))
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols2))
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(barTopicId, 0)))
            .build();

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName("range")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Collections.emptyMap())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1 and member 2.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, expectedMember1.assignedPartitions()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, expectedMember2.assignedPartitions()),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 1),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2),

            // Member 3 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),

            // The subscription metadata hasn't been updated during the conversion, so a new one is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
            )),

            // Newly joining member 3 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, assignor.targetPartitions(memberId1)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, assignor.targetPartitions(memberId3)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 2),

            // Member 3 has no pending revoking partition. Bump its member epoch and transition to UNRELEASED_PARTITIONS.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords, consumerGroupHeartbeatResult.records());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), syncResult.syncFuture.get().errorCode());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, expectedMember2.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId3, 45000);

        // Simulate a failed replay. The context is rolled back and the group is converted back to the classic group.
        context.rollback();
        assertEquals(group, context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false));
    }

    @Test
    public void testClassicGroupOnUnloadedEmptyAndPreparingRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ClassicGroup emptyGroup = context.createClassicGroup("empty-group");
        assertTrue(emptyGroup.isInState(EMPTY));

        ClassicGroup preparingGroup = context.createClassicGroup("preparing-group");
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("preparing-group")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        // preparing-group should have 2 members.
        GroupMetadataManagerTestContext.JoinResult joinResult1 = context.sendClassicGroupJoin(request);
        GroupMetadataManagerTestContext.JoinResult joinResult2 = context.sendClassicGroupJoin(request);

        assertFalse(joinResult1.joinFuture.isDone());
        assertFalse(joinResult2.joinFuture.isDone());
        assertTrue(preparingGroup.isInState(PREPARING_REBALANCE));
        assertEquals(2, preparingGroup.numMembers());

        context.onUnloaded();

        assertTrue(emptyGroup.isInState(DEAD));
        assertTrue(preparingGroup.isInState(DEAD));
        assertTrue(joinResult1.joinFuture.isDone());
        assertTrue(joinResult2.joinFuture.isDone());
        assertEquals(new JoinGroupResponseData()
            .setMemberId(joinResult1.joinFuture.get().memberId())
            .setMembers(Collections.emptyList())
            .setErrorCode(NOT_COORDINATOR.code()), joinResult1.joinFuture.get());

        assertEquals(new JoinGroupResponseData()
            .setMemberId(joinResult2.joinFuture.get().memberId())
            .setMembers(Collections.emptyList())
            .setErrorCode(NOT_COORDINATOR.code()), joinResult2.joinFuture.get());
    }

    @Test
    public void testClassicGroupOnUnloadedCompletingRebalance() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        ClassicGroup group = context.createClassicGroup("group-id");

        // Set up a group in with a leader, follower, and a pending member.
        // Have the pending member join the group and both the pending member
        // and the follower sync. We should have 2 members awaiting sync.
        GroupMetadataManagerTestContext.PendingMemberGroupResult pendingGroupResult = context.setupGroupWithPendingMember(group);
        String pendingMemberId = pendingGroupResult.pendingMemberResponse.memberId();

        // Compete join group for the pending member
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(pendingMemberId)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        assertTrue(joinResult.records.isEmpty());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.NONE.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(3, group.allMembers().size());
        assertEquals(0, group.numPendingJoinMembers());

        // Follower and pending send SyncGroup request.
        // Follower and pending member should be awaiting sync while the leader is pending sync.
        GroupMetadataManagerTestContext.SyncResult followerSyncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(pendingGroupResult.followerId)
                .withGenerationId(joinResult.joinFuture.get().generationId())
                .build());

        GroupMetadataManagerTestContext.SyncResult pendingMemberSyncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(pendingMemberId)
                .withGenerationId(joinResult.joinFuture.get().generationId())
                .build());

        assertFalse(followerSyncResult.syncFuture.isDone());
        assertFalse(pendingMemberSyncResult.syncFuture.isDone());
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        context.onUnloaded();

        assertTrue(group.isInState(DEAD));
        assertTrue(followerSyncResult.syncFuture.isDone());
        assertTrue(pendingMemberSyncResult.syncFuture.isDone());
        assertEquals(new SyncGroupResponseData()
            .setAssignment(EMPTY_ASSIGNMENT)
            .setErrorCode(NOT_COORDINATOR.code()), followerSyncResult.syncFuture.get());
        assertEquals(new SyncGroupResponseData()
            .setAssignment(EMPTY_ASSIGNMENT)
            .setErrorCode(NOT_COORDINATOR.code()), pendingMemberSyncResult.syncFuture.get());
    }

    @Test
    public void testLastConsumerProtocolMemberLeavingConsumerGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
            fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
            barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
        )));

        context.commit();
        ConsumerGroup consumerGroup = context.groupMetadataManager.consumerGroup(groupId);

        // Member 2 leaves the consumer group, triggering the downgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));


        byte[] assignment = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        Map<String, byte[]> assignments = Map.of(memberId1, assignment);

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment
            )
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),

            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId),

            GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments, MetadataVersion.latestTesting())
        );

        assertUnorderedListEquals(expectedRecords.subList(0, 2), result.records().subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 4), result.records().subList(2, 4));
        assertRecordEquals(expectedRecords.get(4), result.records().get(4));
        assertUnorderedListEquals(expectedRecords.subList(5, 7), result.records().subList(5, 7));
        assertRecordsEquals(expectedRecords.subList(7, 10), result.records().subList(7, 10));

        verify(context.metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.STABLE, null);

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<Void, CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);
        // The new rebalance has a groupJoin timeout.
        ScheduledTimeout<Void, CoordinatorRecord> groupJoinTimeout = context.timer.timeout(
            classicGroupJoinKey(groupId)
        );
        assertNotNull(groupJoinTimeout);

        // A new rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));

        // Simulate a failed write to the log.
        context.rollback();

        // The group is reverted back to the consumer group.
        assertEquals(consumerGroup, context.groupMetadataManager.consumerGroup(groupId));
    }

    @Test
    public void testLastConsumerProtocolMemberSessionTimeoutInConsumerGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
            fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
            barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
        )));

        context.commit();

        // Session timer is scheduled on the heartbeat.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // Verify that there is a session timeout.
        context.assertSessionTimeout(groupId, memberId2, 45000);

        // Advance time past the session timeout.
        // Member 2 should be fenced from the group, thus triggering the downgrade.
        ExpiredTimeout<Void, CoordinatorRecord> timeout = context.sleep(45000 + 1).get(0);
        assertEquals(groupSessionTimeoutKey(groupId, memberId2), timeout.key);

        byte[] assignment = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        Map<String, byte[]> assignments = Map.of(memberId1, assignment);

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment
            )
        );
        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),

            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId),

            GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments, MetadataVersion.latestTesting())
        );

        assertUnorderedListEquals(expectedRecords.subList(0, 2), timeout.result.records().subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 4), timeout.result.records().subList(2, 4));
        assertRecordEquals(expectedRecords.get(4), timeout.result.records().get(4));
        assertUnorderedListEquals(expectedRecords.subList(5, 7), timeout.result.records().subList(5, 7));
        assertRecordsEquals(expectedRecords.subList(7, 10), timeout.result.records().subList(7, 10));

        verify(context.metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.STABLE, null);

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<Void, CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);
        // The new rebalance has a groupJoin timeout.
        ScheduledTimeout<Void, CoordinatorRecord> groupJoinTimeout = context.timer.timeout(
            classicGroupJoinKey(groupId)
        );
        assertNotNull(groupJoinTimeout);

        // A new rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testLastConsumerProtocolMemberRebalanceTimeoutInConsumerGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(30000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(30000)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
            fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
            barTopicName, new TopicMetadata(barTopicId, barTopicName, 3),
            zarTopicName, new TopicMetadata(zarTopicId, zarTopicName, 1)
        )));

        context.commit();

        // Prepare the new assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)
            ))
        )));

        // Member 2 heartbeats with a different subscribedTopicNames. The assignor computes a new assignment
        // where member 2 will need to revoke topic partition bar-2 thus transitions to the REVOKING state.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of(
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(fooTopicId)
                        .setPartitions(List.of(3, 4, 5)),
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(barTopicId)
                        .setPartitions(List.of(2))
                ))
        );

        // Verify that there is a rebalance timeout.
        context.assertRebalanceTimeout(groupId, memberId2, 30000);

        // Advance time past the session timeout.
        // Member 2 should be fenced from the group, thus triggering the downgrade.
        ExpiredTimeout<Void, CoordinatorRecord> timeout = context.sleep(30000 + 1).get(0);
        assertEquals(consumerGroupRebalanceTimeoutKey(groupId, memberId2), timeout.key);

        byte[] assignment = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        Map<String, byte[]> assignments = Map.of(memberId1, assignment);

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            11,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment
            )
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),

            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId),

            GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments, MetadataVersion.latestTesting())
        );

        assertUnorderedListEquals(expectedRecords.subList(0, 2), timeout.result.records().subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 4), timeout.result.records().subList(2, 4));
        assertRecordEquals(expectedRecords.get(4), timeout.result.records().get(4));
        assertUnorderedListEquals(expectedRecords.subList(5, 7), timeout.result.records().subList(5, 7));
        assertRecordsEquals(expectedRecords.subList(7, 10), timeout.result.records().subList(7, 10));

        verify(context.metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.RECONCILING, null);

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<Void, CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);
        // The new rebalance has a groupJoin timeout.
        ScheduledTimeout<Void, CoordinatorRecord> groupJoinTimeout = context.timer.timeout(
            classicGroupJoinKey(groupId)
        );
        assertNotNull(groupJoinTimeout);

        // A new rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testLastStaticConsumerProtocolMemberReplacedByClassicProtocolMember() throws ExecutionException, InterruptedException {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String oldMemberId2 = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols1 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols1)
            )
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)))
            .build();
        ConsumerGroupMember oldMember2 = new ConsumerGroupMember.Builder(oldMemberId2)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)))
            .build();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and static member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 2)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(oldMember2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(oldMemberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
            fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
            barTopicName, new TopicMetadata(barTopicId, barTopicName, 2)
        )));
        context.commit();

        // A new member using classic protocol with the same instance id joins, scheduling the downgrade.
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withDefaultProtocolTypeAndProtocols()
            .build();
        GroupMetadataManagerTestContext.JoinResult result = context.sendClassicGroupJoin(joinRequest);
        result.appendFuture.complete(null);
        String newMemberId2 = result.joinFuture.get().memberId();

        ConsumerGroupMember expectedNewConsumerMember2 = new ConsumerGroupMember.Builder(oldMember2, newMemberId2)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .build();
        ConsumerGroupMember expectedNewClassicMember2 = new ConsumerGroupMember.Builder(oldMember2, newMemberId2)
            .setPreviousMemberEpoch(0)
            .setRebalanceTimeoutMs(joinRequest.rebalanceTimeoutMs())
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(joinRequest.sessionTimeoutMs())
                    .setSupportedProtocols(List.of(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                        .setName("range")
                        .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                            List.of(fooTopicName)))))))
            ).build();

        byte[] assignment1 = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        byte[] assignment2 = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 3),
            new TopicPartition(fooTopicName, 4),
            new TopicPartition(fooTopicName, 5)
        ))));
        Map<String, byte[]> assignments = Map.of(
            memberId1, assignment1,
            newMemberId2, assignment2
        );

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment1
            )
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                newMemberId2,
                Optional.ofNullable(oldMember2.instanceId()),
                DEFAULT_CLIENT_ID,
                DEFAULT_CLIENT_ADDRESS.toString(),
                joinRequest.rebalanceTimeoutMs(),
                joinRequest.sessionTimeoutMs(),
                joinRequest.protocolType(),
                joinRequest.protocols(),
                assignment2
            )
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove the existing member 2 that uses the consumer protocol.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, oldMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, oldMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, oldMemberId2),

            // Create the new member 2 that uses the consumer protocol.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedNewConsumerMember2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId2, expectedNewConsumerMember2.assignedPartitions()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedNewConsumerMember2),

            // Update the new member 2 to the member that uses classic protocol.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedNewClassicMember2),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedNewClassicMember2),

            // Remove member 1, member 2 and the consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, newMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, newMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, newMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId),

            // Create the classic group.
            GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments, MetadataVersion.latestTesting())
        );

        assertEquals(expectedRecords.size(), result.records.size());
        assertRecordsEquals(expectedRecords.subList(0, 8), result.records.subList(0, 8));
        assertUnorderedListEquals(expectedRecords.subList(8, 10), result.records.subList(8, 10));
        assertUnorderedListEquals(expectedRecords.subList(10, 12), result.records.subList(10, 12));
        assertRecordEquals(expectedRecords.get(12), result.records.get(12));
        assertUnorderedListEquals(expectedRecords.subList(13, 15), result.records.subList(13, 15));
        assertRecordsEquals(expectedRecords.subList(15, 17), result.records.subList(15, 17));

        // Leader can be either member 1 or member 2.
        try {
            assertRecordEquals(expectedRecords.get(17), result.records.get(17));
        } catch (AssertionFailedError e) {
            expectedClassicGroup.setLeaderId(Optional.of(newMemberId2));
            assertRecordEquals(
                GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments, MetadataVersion.latestTesting()),
                result.records.get(9)
            );
        }

        verify(context.metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.STABLE, null);

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<Void, CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);

        // No rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(STABLE));
    }

    @Test
    public void testJoiningConsumerGroupThrowsExceptionIfGroupOverMaxSize() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .build()))
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, 1)
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        Exception ex = assertThrows(GroupMaxSizeReachedException.class, () -> context.sendClassicGroupJoin(request));
        assertEquals("The consumer group has reached its maximum capacity of 1 members.", ex.getMessage());
    }

    @Test
    public void testJoiningConsumerGroupThrowsExceptionIfProtocolIsNotSupported() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toProtocols("roundrobin")
                            ))
                    )
                    .build()))
            .build();

        JoinGroupRequestData requestWithEmptyProtocols = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withDefaultProtocolTypeAndProtocols()
            .build();
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupJoin(requestWithEmptyProtocols));

        JoinGroupRequestData requestWithInvalidProtocolType = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("connect")
            .withDefaultProtocolTypeAndProtocols()
            .build();
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupJoin(requestWithInvalidProtocolType));
    }

    @Test
    public void testJoiningConsumerGroupWithNewDynamicMember() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        for (short version = ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION; version <= ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION; version++) {
            String memberId = Uuid.randomUuid().toString();
            MockPartitionAssignor assignor = new MockPartitionAssignor("range");
            GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
                .withMetadataImage(new MetadataImageBuilder()
                    .addTopic(fooTopicId, fooTopicName, 2)
                    .addTopic(barTopicId, barTopicName, 1)
                    .addRacks()
                    .build())
                .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                    .withSubscriptionMetadata(
                        Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2))
                    )
                    .withMember(new ConsumerGroupMember.Builder(memberId)
                        .setState(MemberState.STABLE)
                        .setMemberEpoch(10)
                        .setPreviousMemberEpoch(10)
                        .setAssignedPartitions(mkAssignment(
                            mkTopicAssignment(fooTopicId, 0, 1)))
                        .build())
                    .withAssignment(memberId, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)))
                    .withAssignmentEpoch(10))
                .build();

            JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                    List.of(fooTopicName, barTopicName),
                    Collections.emptyList(),
                    version))
                .build();

            // The first round of join request gets the new member id.
            GroupMetadataManagerTestContext.JoinResult firstJoinResult = context.sendClassicGroupJoin(
                request,
                true
            );
            assertTrue(firstJoinResult.records.isEmpty());
            // Simulate a successful write to the log.
            firstJoinResult.appendFuture.complete(null);

            assertTrue(firstJoinResult.joinFuture.isDone());
            assertEquals(Errors.MEMBER_ID_REQUIRED.code(), firstJoinResult.joinFuture.get().errorCode());
            String newMemberId = firstJoinResult.joinFuture.get().memberId();
            assertNotEquals("", newMemberId);

            assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
                memberId, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0)
                )),
                newMemberId, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(barTopicId, 0)
                ))
            )));

            JoinGroupRequestData secondRequest = new JoinGroupRequestData()
                .setGroupId(request.groupId())
                .setMemberId(newMemberId)
                .setProtocolType(request.protocolType())
                .setProtocols(request.protocols())
                .setSessionTimeoutMs(request.sessionTimeoutMs())
                .setRebalanceTimeoutMs(request.rebalanceTimeoutMs())
                .setReason(request.reason());

            // Send second join group request for a new dynamic member with the new member id.
            GroupMetadataManagerTestContext.JoinResult secondJoinResult = context.sendClassicGroupJoin(
                secondRequest,
                true
            );

            ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(newMemberId)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .setState(MemberState.STABLE)
                .setClientId(DEFAULT_CLIENT_ID)
                .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setRebalanceTimeoutMs(500)
                .setAssignedPartitions(assignor.targetPartitions(newMemberId))
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(request.sessionTimeoutMs())
                        .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(request.protocols()))
                )
                .build();

            List<CoordinatorRecord> expectedRecords = List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
                GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
                )),
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),

                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, assignor.targetPartitions(memberId)),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, assignor.targetPartitions(newMemberId)),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),

                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
            );
            assertRecordsEquals(expectedRecords.subList(0, 3), secondJoinResult.records.subList(0, 3));
            assertUnorderedListEquals(expectedRecords.subList(3, 5), secondJoinResult.records.subList(3, 5));
            assertRecordsEquals(expectedRecords.subList(5, 7), secondJoinResult.records.subList(5, 7));

            secondJoinResult.appendFuture.complete(null);
            assertTrue(secondJoinResult.joinFuture.isDone());
            assertEquals(
                new JoinGroupResponseData()
                    .setMemberId(newMemberId)
                    .setGenerationId(11)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setProtocolName("range"),
                secondJoinResult.joinFuture.get()
            );

            context.assertSessionTimeout(groupId, newMemberId, request.sessionTimeoutMs());
            context.assertSyncTimeout(groupId, newMemberId, request.rebalanceTimeoutMs());
        }
    }

    @Test
    public void testJoiningConsumerGroupFailingToPersistRecords() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        String memberId = Uuid.randomUuid().toString();
        String newMemberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            newMemberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withSubscriptionMetadata(
                    Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2))
                )
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10))
            .build();
        context.commit();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(newMemberId)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName),
                Collections.emptyList()))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        // Simulate a failed write to the log.
        joinResult.appendFuture.completeExceptionally(new NotLeaderOrFollowerException());
        context.rollback();

        context.assertNoSessionTimeout(groupId, newMemberId);
        context.assertNoSyncTimeout(groupId, newMemberId);
        assertFalse(context.groupMetadataManager.consumerGroup(groupId).hasMember(newMemberId));
    }

    @Test
    public void testJoiningConsumerGroupWithNewStaticMember() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        String memberId = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addTopic(barTopicId, barTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withSubscriptionMetadata(
                    Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2))
                )
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName),
                Collections.emptyList()))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        // Simulate a successful write to log.
        joinResult.appendFuture.complete(null);
        String newMemberId = joinResult.joinFuture.get().memberId();
        assertNotEquals("", newMemberId);

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(newMemberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(500)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(request.protocols()))
            )
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, Collections.emptyMap()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );
        assertRecordsEquals(expectedRecords, joinResult.records);

        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(newMemberId)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResult.joinFuture.get()
        );

        context.assertSessionTimeout(groupId, newMemberId, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, newMemberId, request.rebalanceTimeoutMs());
    }

    @Test
    public void testJoiningConsumerGroupReplacingExistingStaticMember() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        String memberId = Uuid.randomUuid().toString();
        String instanceId = "instance-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DISABLED.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withSubscriptionMetadata(
                    Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2))
                )
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setInstanceId(instanceId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setSubscribedTopicNames(List.of(fooTopicName))
                    .setRebalanceTimeoutMs(500)
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10))
            .build();
        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName),
                Collections.emptyList()))
            .build();

        // The static member joins with UNKNOWN_MEMBER_ID.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request,
            true
        );

        // Simulate a successful write to log.
        joinResult.appendFuture.complete(null);
        String newMemberId = joinResult.joinFuture.get().memberId();
        assertNotEquals("", newMemberId);

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(newMemberId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)))
            .setRebalanceTimeoutMs(500)
            .build();

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(newMemberId)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)))
            .setRebalanceTimeoutMs(500)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(request.protocols())))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove the old static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

            // Replace the old static member by the new static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember),

            // Updated the new static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );
        assertRecordsEquals(expectedRecords, joinResult.records);
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(newMemberId)
                .setGenerationId(10)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResult.joinFuture.get()
        );

        context.assertSessionTimeout(groupId, newMemberId, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, newMemberId, request.rebalanceTimeoutMs());
    }

    @Test
    public void testJoiningConsumerGroupWithExistingStaticMemberAndNewSubscription() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addTopic(barTopicId, barTopicName, 1)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withSubscriptionMetadata(Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 1),
                    zarTopicName, new TopicMetadata(zarTopicId, zarTopicName, 1)
                ))
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setInstanceId(instanceId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0),
                        mkTopicAssignment(barTopicId, 0)))
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toConsumerProtocol(
                                    List.of(fooTopicName, barTopicName),
                                    List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(fooTopicName, 1))
                                )
                            ))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0),
                    mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .withAssignmentEpoch(10))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 11);

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(zarTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0),
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        // Member 1 rejoins with a new subscription list.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                Collections.emptyList()))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId1)
            .setInstanceId(instanceId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setRebalanceTimeoutMs(500)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName, zarTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(zarTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            Collections.emptyList()
                        )
                    ))
            )
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(zarTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(barTopicId, 0),
                mkTopicAssignment(fooTopicId, 1))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );
        assertRecordsEquals(expectedRecords.subList(0, 2), joinResult.records.subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 4), joinResult.records.subList(2, 4));
        assertRecordsEquals(expectedRecords.subList(4, 6), joinResult.records.subList(4, 6));

        joinResult.appendFuture.complete(null);
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResult.joinFuture.get()
        );
        context.assertSessionTimeout(groupId, memberId1, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request.rebalanceTimeoutMs());
    }

    @Test
    public void testStaticMemberJoiningConsumerGroupWithUnknownInstanceId() throws Exception {
        String groupId = "group-id";
        String instanceId = "instance-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String fooTopicName = "foo";
        String barTopicName = "bar";

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(fooTopicName, 1))
            );
        // Set up a ConsumerGroup with no static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .build()))
            .build();

        // The member joins with an instance id.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withGroupInstanceId(instanceId)
            .withProtocols(protocols)
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupJoin(request));
    }

    @Test
    public void testStaticMemberJoiningConsumerGroupWithUnmatchedMemberId() throws Exception {
        String groupId = "group-id";
        String instanceId = "instance-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String fooTopicName = "foo";
        String barTopicName = "bar";

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(fooTopicName, 1))
            );
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setInstanceId(instanceId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .build()))
            .build();

        // The member joins with the same instance id and a different member id.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(Uuid.randomUuid().toString())
            .withGroupInstanceId(instanceId)
            .withProtocols(protocols)
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.sendClassicGroupJoin(request));
    }

    @Test
    public void testReconciliationInJoiningConsumerGroupWithEagerProtocol() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addTopic(barTopicId, barTopicName, 1)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withSubscriptionMetadata(Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
                ))
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0),
                        mkTopicAssignment(barTopicId, 0)))
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toConsumerProtocol(
                                    List.of(fooTopicName, barTopicName),
                                    List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))
                                )
                            ))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0),
                    mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .withAssignmentEpoch(10))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 11);

        // Prepare the new target assignment.
        // Member 1 will need to revoke bar-0, and member 2 will need to revoke foo-1.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            ))
        )));

        // Member 1 rejoins with a new subscription list and an empty owned
        // partition, and transitions to UNRELEASED_PARTITIONS.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                Collections.emptyList()))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult1 = context.sendClassicGroupJoin(request);

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setRebalanceTimeoutMs(500)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName, zarTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(zarTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            Collections.emptyList()
                        )
                    ))
            )
            .build();

        List<CoordinatorRecord> expectedRecords1 = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 1),
                zarTopicName, new TopicMetadata(zarTopicId, zarTopicName, 1)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(barTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );
        assertEquals(expectedRecords1.size(), joinResult1.records.size());
        assertRecordsEquals(expectedRecords1.subList(0, 3), joinResult1.records.subList(0, 3));
        assertUnorderedListEquals(expectedRecords1.subList(3, 5), joinResult1.records.subList(3, 5));
        assertRecordsEquals(expectedRecords1.subList(5, 7), joinResult1.records.subList(5, 7));

        assertEquals(expectedMember1.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult1.appendFuture.complete(null);
        JoinGroupResponseData joinResponse1 = joinResult1.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse1
        );
        context.assertSessionTimeout(groupId, memberId1, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse1.memberId(),
            joinResponse1.generationId(),
            joinResponse1.protocolName(),
            joinResponse1.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(zarTopicName, 0)
            )
        );

        // Member 2 heartbeats to confirm revoking foo-1.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setTopicPartitions(Collections.emptyList())
        );

        // Member 1 heartbeats to be notified to rejoin.
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setGenerationId(joinResponse1.generationId())
            ).response().errorCode()
        );
        context.assertJoinTimeout(groupId, memberId1, 500);

        // Member 1 rejoins to transition from UNRELEASED_PARTITIONS to STABLE.
        GroupMetadataManagerTestContext.JoinResult joinResult2 = context.sendClassicGroupJoin(request);
        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(expectedMember1)
            .setState(MemberState.STABLE)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0)))
            .build();

        assertRecordsEquals(
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)),
            joinResult2.records
        );
        assertEquals(expectedMember2.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult2.appendFuture.complete(null);
        context.assertNoJoinTimeout(groupId, memberId1);
        JoinGroupResponseData joinResponse2 = joinResult2.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse2
        );
        context.assertSessionTimeout(groupId, memberId1, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse2.memberId(),
            joinResponse2.generationId(),
            joinResponse2.protocolName(),
            joinResponse2.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1),
                new TopicPartition(zarTopicName, 0)
            )
        );
    }

    @Test
    public void testReconciliationInJoiningConsumerGroupWithCooperativeProtocol() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addTopic(barTopicId, barTopicName, 1)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withSubscriptionMetadata(Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
                ))
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0),
                        mkTopicAssignment(barTopicId, 0)))
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toConsumerProtocol(
                                    List.of(fooTopicName, barTopicName),
                                    List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))
                                )
                            ))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0),
                    mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .withAssignmentEpoch(10))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 11);

        // Prepare the new target assignment.
        // Member 1 will need to revoke bar-0, and member 2 will need to revoke foo-1.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            ))
        )));

        // Member 1 rejoins with a new subscription list and transitions to UNREVOKED_PARTITIONS.
        JoinGroupRequestData request1 = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult1 = context.sendClassicGroupJoin(request1);

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setRebalanceTimeoutMs(500)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName, zarTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(barTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request1.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))
                        )
                    ))
            )
            .build();

        List<CoordinatorRecord> expectedRecords1 = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 1),
                zarTopicName, new TopicMetadata(zarTopicId, zarTopicName, 1)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(barTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 11),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );
        assertEquals(expectedRecords1.size(), joinResult1.records.size());
        assertRecordsEquals(expectedRecords1.subList(0, 3), joinResult1.records.subList(0, 3));
        assertUnorderedListEquals(expectedRecords1.subList(3, 5), joinResult1.records.subList(3, 5));
        assertRecordsEquals(expectedRecords1.subList(5, 7), joinResult1.records.subList(5, 7));

        assertEquals(expectedMember1.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult1.appendFuture.complete(null);
        JoinGroupResponseData joinResponse1 = joinResult1.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(10)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse1
        );
        context.assertSessionTimeout(groupId, memberId1, request1.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request1.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse1.memberId(),
            joinResponse1.generationId(),
            joinResponse1.protocolName(),
            joinResponse1.protocolType(),
            List.of(new TopicPartition(fooTopicName, 0))
        );

        // Member 1 heartbeats to be notified to rejoin.
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setGenerationId(joinResponse1.generationId())
            ).response().errorCode()
        );
        context.assertJoinTimeout(groupId, memberId1, 500);

        // Member 1 rejoins to transition from UNREVOKED_PARTITIONS to UNRELEASED_PARTITIONS.
        JoinGroupRequestData request2 = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of(new TopicPartition(fooTopicName, 0))))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult2 = context.sendClassicGroupJoin(request2);

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(expectedMember1)
            .setMemberEpoch(11)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setPartitionsPendingRevocation(Collections.emptyMap())
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(zarTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request2.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of(new TopicPartition(fooTopicName, 0))
                        )
                    ))
            )
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            joinResult2.records
        );
        assertEquals(expectedMember2.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult2.appendFuture.complete(null);
        context.assertNoJoinTimeout(groupId, memberId1);
        JoinGroupResponseData joinResponse2 = joinResult2.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse2
        );
        context.assertSessionTimeout(groupId, memberId1, request2.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request2.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse2.memberId(),
            joinResponse2.generationId(),
            joinResponse2.protocolName(),
            joinResponse2.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(zarTopicName, 0)
            )
        );

        // Member 2 heartbeats to confirm revoking foo-1.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setTopicPartitions(Collections.emptyList())
        );

        // Member 1 heartbeats to be notified to rejoin.
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setGenerationId(joinResponse2.generationId())
            ).response().errorCode()
        );
        context.assertJoinTimeout(groupId, memberId1, 500);

        // Member 1 rejoins to transition from UNRELEASED_PARTITIONS to STABLE.
        JoinGroupRequestData request3 = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(zarTopicName, 0))))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult3 = context.sendClassicGroupJoin(request3);

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(expectedMember2)
            .setState(MemberState.STABLE)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request3.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(zarTopicName, 0))
                        )
                    ))
            )
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
            ),
            joinResult3.records
        );
        assertEquals(expectedMember3.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult3.appendFuture.complete(null);
        context.assertNoJoinTimeout(groupId, memberId1);
        JoinGroupResponseData joinResponse3 = joinResult3.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse3
        );
        context.assertSessionTimeout(groupId, memberId1, request3.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request3.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse3.memberId(),
            joinResponse3.generationId(),
            joinResponse3.protocolName(),
            joinResponse3.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1),
                new TopicPartition(zarTopicName, 0)
            )
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithAllConsumerProtocolVersions() throws Exception {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        for (short version = ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION; version <= ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION; version++) {
            List<TopicPartition> topicPartitions = List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1),
                new TopicPartition(fooTopicName, 2),
                new TopicPartition(barTopicName, 0),
                new TopicPartition(barTopicName, 1)
            );

            List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
                new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                    .setName("range")
                    .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                        new ConsumerPartitionAssignor.Subscription(
                            List.of(fooTopicName, barTopicName),
                            null,
                            topicPartitions
                        ),
                        version
                    )))
            );

            ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
                .setState(MemberState.STABLE)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(5000)
                        .setSupportedProtocols(protocols)
                )
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .build();
            ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
                .setState(MemberState.STABLE)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .build();

            // Consumer group with two members.
            // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
            GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
                .withMetadataImage(new MetadataImageBuilder()
                    .addTopic(fooTopicId, fooTopicName, 6)
                    .addTopic(barTopicId, barTopicName, 3)
                    .addRacks()
                    .build())
                .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                    .withMember(member1)
                    .withMember(member2)
                    .withAssignment(memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .withAssignment(memberId2, mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .withAssignmentEpoch(10))
                .build();

            context.verifyClassicGroupSyncToConsumerGroup(
                groupId,
                memberId1,
                10,
                "range",
                ConsumerProtocol.PROTOCOL_TYPE,
                topicPartitions,
                version
            );
        }
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithUnknownMemberId() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        // Consumer group with a member that doesn't use the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .build()))
            .build();

        // Request with unknown member id.
        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(Uuid.randomUuid().toString())
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );

        // Request with unknown instance id.
        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGroupInstanceId("unknown-instance-id")
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );

        // Request with member id that doesn't use the classic protocol.
        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithFencedInstanceId() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        // Consumer group with a static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setInstanceId(instanceId)
                    .build()))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(Uuid.randomUuid().toString())
                .withGroupInstanceId(instanceId)
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithInconsistentGroupProtocol() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        Collections.emptyList()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build()))
            .build();

        // Request with unmatched protocol name.
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolName("roundrobin")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );

        // Request with unmatched protocol type.
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType("connect")
                .build())
        );

        // Request with null protocol type or null protocol name won't fail the validation.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            memberId,
            10,
            null,
            null,
            Collections.emptyList()
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithIllegalGeneration() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        Collections.emptyList()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build()))
            .build();

        assertThrows(IllegalGenerationException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(9)
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .withProtocolName("range")
                .build())
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupRebalanceInProgress() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        Collections.emptyList()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        // The group epoch is greater than the member epoch.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 11)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setRebalanceTimeoutMs(10000)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build()))
            .build();

        assertThrows(RebalanceInProgressException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .withProtocolName("range")
                .build())
        );
        context.assertJoinTimeout(groupId, memberId, 10000);
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerGroupMaintainsSession() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int sessionTimeout = 5000;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        Collections.emptyList()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
            .withMember(new ConsumerGroupMember.Builder(memberId)
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(sessionTimeout)
                        .setSupportedProtocols(protocols)
                )
                .setMemberEpoch(10)
                .build()))
            .build();

        // Heartbeat to schedule the session timeout.
        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setGenerationId(10);
        context.sendClassicGroupHeartbeat(request);
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);

        // Advance clock by 1/2 of session timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(sessionTimeout / 2));

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(request).response();
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);

        // Advance clock by 1/2 of session timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(sessionTimeout / 2));

        heartbeatResponse = context.sendClassicGroupHeartbeat(request).response();
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerGroupRebalanceInProgress() throws Exception {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        int sessionTimeout = 5000;
        int rebalanceTimeout = 10000;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        Collections.emptyList()
                    )
                )))
        );

        // Member 1 has a member epoch smaller than the group epoch.
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeout)
                    .setSupportedProtocols(protocols)
            )
            .setMemberEpoch(9)
            .build();

        // Member 2 has unrevoked partition.
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setPartitionsPendingRevocation(mkAssignment(mkTopicAssignment(fooTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeout)
                    .setSupportedProtocols(protocols)
            )
            .setMemberEpoch(10)
            .build();

        // Member 3 is in UNRELEASED_PARTITIONS and all the partitions in its target assignment are free.
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder(memberId3)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(barTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeout)
                    .setSupportedProtocols(protocols)
            )
            .setMemberEpoch(10)
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withMember(member3)
                .withAssignment(memberId3, mkAssignment(mkTopicAssignment(barTopicId, 0, 1, 2))))
            .build();

        List.of(memberId1, memberId2, memberId3).forEach(memberId -> {
            CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> heartbeatResult = context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setGenerationId(memberId.equals(memberId1) ? 9 : 10)
            );
            assertEquals(Collections.emptyList(), heartbeatResult.records());
            assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResult.response().errorCode());
            context.assertSessionTimeout(groupId, memberId, sessionTimeout);
            context.assertJoinTimeout(groupId, memberId, rebalanceTimeout);
        });
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithUnknownMember() {
        String groupId = "group-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-member-id")
                .setGenerationId(10)
        ));

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-member-id")
                .setGroupInstanceId("unknown-instance-id")
                .setGenerationId(10)
        ));
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithFencedInstanceId() {
        String groupId = "group-id";
        String memberId = "member-id";
        String instanceId = "instance-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setInstanceId(instanceId)
                    .setMemberEpoch(10)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(Collections.emptyList())
                    )
                    .build()))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-member-id")
                .setGroupInstanceId(instanceId)
                .setGenerationId(10)
        ));
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithIllegalGenerationId() {
        String groupId = "group-id";
        String memberId = "member-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(Collections.emptyList())
                    )
                    .build()))
            .build();

        assertThrows(IllegalGenerationException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setGenerationId(9)
        ));
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithMemberNotUsingClassicProtocol() {
        String groupId = "group-id";
        String memberId = "member-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .build()))
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setGenerationId(10)
        ));
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolFencedWhenSessionTimeout() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int sessionTimeout = 5000;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        Collections.emptyList()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(sessionTimeout)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build()))
            .build();

        // Heartbeat to schedule the session timeout.
        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setGenerationId(10);
        context.sendClassicGroupHeartbeat(request);
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);

        // Advance clock by session timeout + 1.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(sessionTimeout + 1);

        // The member is fenced from the group.
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, CoordinatorRecord> timeout = timeouts.get(0);
        assertEquals(groupSessionTimeoutKey(groupId, memberId), timeout.key);
        assertRecordsEquals(
            List.of(
                // The member is removed.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
            ),
            timeout.result.records()
        );
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolFencedWhenJoinTimeout() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int rebalanceTimeout = 500;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        Collections.emptyList()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol whose member epoch is smaller than the group epoch.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setRebalanceTimeoutMs(rebalanceTimeout)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(9)
                    .build()))
            .build();

        // Heartbeat to schedule the join timeout.
        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setGenerationId(9);
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(request).response().errorCode()
        );
        context.assertSessionTimeout(groupId, memberId, 5000);
        context.assertJoinTimeout(groupId, memberId, rebalanceTimeout);

        // Advance clock by rebalance timeout + 1.
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(rebalanceTimeout + 1);

        // The member is fenced from the group.
        assertEquals(1, timeouts.size());
        ExpiredTimeout<Void, CoordinatorRecord> timeout = timeouts.get(0);
        assertEquals(consumerGroupJoinKey(groupId, memberId), timeout.key);
        assertRecordsEquals(
            List.of(
                // The member is removed.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
            ),
            timeout.result.records()
        );
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolBatchLeaveGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String instanceId2 = "instance-id-2";
        String instanceId3 = "instance-id-3";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol1 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 0))
                ))))
        );
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol2 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 1))
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol1)
            )
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 0)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId(instanceId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(9)
            .setPreviousMemberEpoch(8)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol2)
            )
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1)))
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder(memberId3)
            .setInstanceId(instanceId3)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(barTopicId, 0)))
            .build();

        // Consumer group with three members.
        // Dynamic member 1 uses the classic protocol.
        // Static member 2 uses the classic protocol.
        // Static member 3 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addTopic(barTopicId, barTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withMember(member3)
                .withAssignment(memberId1, mkAssignment(mkTopicAssignment(fooTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(mkTopicAssignment(fooTopicId, 1)))
                .withAssignment(memberId3, mkAssignment(mkTopicAssignment(barTopicId, 0)))
                .withAssignmentEpoch(10))
            .build();
        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
            fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
            barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
        )));

        // Member 1 joins to schedule the sync timeout and the heartbeat timeout.
        context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId1)
                .withRebalanceTimeoutMs(member1.rebalanceTimeoutMs())
                .withSessionTimeoutMs(member1.classicMemberMetadata().get().sessionTimeoutMs())
                .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                    List.of(fooTopicName, barTopicName),
                    List.of(new TopicPartition(fooTopicName, 0))))
                .build()
        ).appendFuture.complete(null);
        context.assertSyncTimeout(groupId, memberId1, member1.rebalanceTimeoutMs());
        context.assertSessionTimeout(groupId, memberId1, member1.classicMemberMetadata().get().sessionTimeoutMs());

        // Member 2 heartbeats to schedule the join timeout and the heartbeat timeout.
        context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setGenerationId(9)
        );
        context.assertJoinTimeout(groupId, memberId2, member2.rebalanceTimeoutMs());
        context.assertSessionTimeout(groupId, memberId2, member2.classicMemberMetadata().get().sessionTimeoutMs());

        // Member 1 and member 2 leave the group.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    // Valid member id.
                    new MemberIdentity()
                        .setMemberId(memberId1),
                    new MemberIdentity()
                        .setGroupInstanceId(instanceId2),
                    // Member that doesn't use the classic protocol.
                    new MemberIdentity()
                        .setMemberId(memberId3)
                        .setGroupInstanceId(instanceId3),
                    // Unknown member id.
                    new MemberIdentity()
                        .setMemberId("unknown-member-id"),
                    new MemberIdentity()
                        .setGroupInstanceId("unknown-instance-id"),
                    // Fenced instance id.
                    new MemberIdentity()
                        .setMemberId("unknown-member-id")
                        .setGroupInstanceId(instanceId3)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId(memberId1),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(instanceId2)
                        .setMemberId(memberId2),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(instanceId3)
                        .setMemberId(memberId3)
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId("unknown-member-id")
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId("unknown-instance-id")
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(instanceId3)
                        .setMemberId("unknown-member-id")
                        .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
                )),
            leaveResult.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove member 1
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            // Remove member 2.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Bump the group epoch.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
        );
        assertEquals(expectedRecords, leaveResult.records());

        context.assertNoSessionTimeout(groupId, memberId1);
        context.assertNoSyncTimeout(groupId, memberId1);
        context.assertNoSessionTimeout(groupId, memberId2);
        context.assertNoJoinTimeout(groupId, memberId2);
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolBatchLeaveGroupUpdatingSubscriptionMetadata() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 0))
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol)
            )
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 0)))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(barTopicId, 0)))
            .build();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addTopic(barTopicId, barTopicName, 1)
                .addRacks()
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(mkTopicAssignment(fooTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(mkTopicAssignment(barTopicId, 0)))
                .withAssignmentEpoch(10))
            .build();
        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
            fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2),
            barTopicName, new TopicMetadata(barTopicId, barTopicName, 1)
        )));

        // Member 1 leaves the group.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId(memberId1)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId(memberId1)
                )),
            leaveResult.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove member 1
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            // Update the subscription metadata.
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId,
                Map.of(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 2))
            ),
            // Bump the group epoch.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
        );
        assertEquals(expectedRecords, leaveResult.records());
    }

    @Test
    public void testClassicGroupLeaveToConsumerGroupWithoutValidLeaveGroupMember() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        // Consumer group without member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .build()))
            .build();

        // Send leave request without valid member.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId("unknown-member-id"),
                    new MemberIdentity()
                        .setMemberId(memberId)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId("unknown-member-id")
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId(memberId)
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                )),
            leaveResult.response()
        );

        assertEquals(Collections.emptyList(), leaveResult.records());
    }

    @Test
    public void testNoConversionWhenSizeExceedsClassicMaxGroupSize() throws Exception {
        String groupId = "group-id";
        String nonClassicMemberId = "1";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(new byte[0])
        );

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(nonClassicMemberId).build();
        ConsumerGroupMember classicMember1 = new ConsumerGroupMember.Builder("2")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata().setSupportedProtocols(protocols))
            .build();
        ConsumerGroupMember classicMember2 = new ConsumerGroupMember.Builder("3")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata().setSupportedProtocols(protocols))
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, 1)
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(
                new ConsumerGroupBuilder(groupId, 10)
                    .withMember(member)
                    .withMember(classicMember1)
                    .withMember(classicMember2)
            )
            .build();

        assertEquals(Group.GroupType.CONSUMER, context.groupMetadataManager.group(groupId).type());

        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(nonClassicMemberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
        );

        assertEquals(Group.GroupType.CONSUMER, context.groupMetadataManager.group(groupId).type());
    }

    @Test
    public void testShareGroupHeartbeatRequestValidation() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();
        Exception ex;

        // MemberId must be present in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()));
        assertEquals("MemberId can't be empty.", ex.getMessage());

        // GroupId must be present in all requests.
        String memberId = Uuid.randomUuid().toString();
        ex = assertThrows(InvalidRequestException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setMemberId(memberId)));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // GroupId can't be all whitespaces.
        ex = assertThrows(InvalidRequestException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId("   ")));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // SubscribedTopicNames must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId("foo")
                .setMemberEpoch(0)));
        assertEquals("SubscribedTopicNames must be set in first request.", ex.getMessage());

        // MemberId must be non-empty in all requests except for the first one where it
        // could be empty (epoch != 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(1)));
        assertEquals("MemberId can't be empty.", ex.getMessage());

        // RackId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId("foo")
                .setMemberEpoch(1)
                .setRackId("")));
        assertEquals("RackId can't be empty.", ex.getMessage());
    }

    @Test
    public void testShareGroupDescribeRequest() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        // GroupId is not required
        List<ShareGroupDescribeResponseData.DescribedGroup> groups = context.sendShareGroupDescribe(Collections.emptyList());
        assertEquals(0, groups.size());

        // Group id not found
        groups = context.sendShareGroupDescribe(List.of("unknown-group"));
        assertEquals(1, groups.size());
        assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), groups.get(0).errorCode());
    }

    @Test
    public void testShareGroupDescribeNoErrors() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("share-range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.emptyMap()
        ));

        List<String> groupIds = List.of("group-id-1", "group-id-2");
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupIds.get(0), 100));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupIds.get(1), 15));

        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupIds.get(1))
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo")));

        // Verify that a member id was generated for the new member.
        String memberId = result.response().memberId();
        assertNotNull(memberId);
        context.commit();

        List<ShareGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(100)
                .setGroupId(groupIds.get(0))
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setAssignorName("share-range"),
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(16)
                .setAssignmentEpoch(16)
                .setGroupId(groupIds.get(1))
                .setMembers(List.of(
                    new ShareGroupMember.Builder(memberId)
                        .setMemberEpoch(16)
                        .setClientId("client")
                        .setClientHost("localhost/127.0.0.1")
                        .setSubscribedTopicNames(List.of("foo"))
                        .build()
                        .asShareGroupDescribeMember(
                            new MetadataImageBuilder().build().topics()
                        )
                ))
                .setGroupState(ShareGroup.ShareGroupState.STABLE.toString())
                .setAssignorName("share-range")
        );
        List<ShareGroupDescribeResponseData.DescribedGroup> actual = context.sendShareGroupDescribe(groupIds);

        assertEquals(expected, actual);
    }

    @Test
    public void testShareGroupMemberIdGeneration() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(MetadataImage.EMPTY)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.emptyMap()
        ));

        String memberId = Uuid.randomUuid().toString();
        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId("group-foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar")));

        assertEquals(
            memberId,
            result.response().memberId(),
            "MemberId should remain unchanged, as the server does not generate a new one since the consumer generates its own."
        );

        // The response should get a bumped epoch and should not
        // contain any assignment because we did not provide
        // topics metadata.
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()),
            result.response()
        );
    }

    @Test
    public void testShareGroupUnknownGroupId() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        assertThrows(IllegalStateException.class, () ->
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(100) // Epoch must be > 0.
                    .setSubscribedTopicNames(List.of("foo", "bar"))));
    }

    @Test
    public void testShareGroupUnknownMemberIdJoins() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(new NoOpPartitionAssignor())
            .build();

        // A first member joins to create the group.
        context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar")));

        // The second member is rejected because the member id is unknown and
        // the member epoch is not zero.
        assertThrows(UnknownMemberIdException.class, () ->
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));
    }

    @Test
    public void testShareGroupMemberJoinsEmptyGroupWithAssignments() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.shareGroup(groupId));

        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar")));

        assertResponseEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            result.response()
        );

        ShareGroupMember expectedMember = new ShareGroupMember.Builder(memberId)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            ))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newShareGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
            )),
            GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentEpochRecord(groupId, 1),
            GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testShareGroupLeavingMemberBumpsGroupEpoch() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addTopic(zarTopicId, zarTopicName, 1)
                .addRacks()
                .build())
            .withShareGroup(new ShareGroupBuilder(groupId, 10)
                .withMember(new ShareGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ShareGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setSubscribedTopicNames(List.of("foo", "bar")));

        assertResponseEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH),
            result.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Subscription metadata is recomputed because zar is no longer there.
            GroupCoordinatorRecordHelpers.newShareGroupSubscriptionMetadataRecord(groupId, Map.of(
                fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
            )),
            GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 11)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testShareGroupNewMemberIsRejectedWithMaximumMembersIsReached() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        // A share group cannot have pre-defined members and member metadata as members and assignments
        // are not persisted.
        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withConfig(GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG, 1)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.emptyMap()
        ));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 100));

        // Member 1 joins the group.
        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar")));
        assertEquals(101, result.response().memberEpoch());

        // Member 2 joins the group.
        assertThrows(GroupMaxSizeReachedException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar"))));
    }

    @Test
    public void testShareGroupDelete() {
        String groupId = "share-group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroup(new ShareGroupBuilder(groupId, 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentEpochTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newShareGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newShareGroupEpochTombstoneRecord(groupId)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.createGroupTombstoneRecords("share-group-id", records);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void testShareGroupStates() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("share-range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withShareGroup(new ShareGroupBuilder(groupId, 10))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 10));

        assertEquals(ShareGroup.ShareGroupState.EMPTY, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, new ShareGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .build()));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 11));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3))));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentEpochRecord(groupId, 11));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, new ShareGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2)))
            .build()));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, new ShareGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build()));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));
    }

    @Test
    public void testConsumerGroupDynamicConfigs() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(Collections.emptyList()));
        assertEquals(1, result.response().memberEpoch());

        // Verify heartbeat interval
        assertEquals(5000, result.response().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Dynamic update group config
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 50000);
        newGroupConfig.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        context.updateGroupConfig(groupId, newGroupConfig);

        // Session timer is rescheduled on second heartbeat.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));
        assertEquals(1, result.response().memberEpoch());

        // Verify heartbeat interval
        assertEquals(10000, result.response().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 50000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testShareGroupDynamicConfigs() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setSubscribedTopicNames(List.of("foo")));
        assertEquals(1, result.response().memberEpoch());

        // Verify heartbeat interval
        assertEquals(5000, result.response().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Dynamic update group config
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(SHARE_SESSION_TIMEOUT_MS_CONFIG, 50000);
        newGroupConfig.put(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        context.updateGroupConfig(groupId, newGroupConfig);

        // Session timer is rescheduled on second heartbeat.
        result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));
        assertEquals(1, result.response().memberEpoch());

        // Verify heartbeat interval
        assertEquals(10000, result.response().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 50000);

        // Advance time.
        assertEquals(
            Collections.emptyList(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testReplayConsumerGroupMemberMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setClientId("clientid")
            .setClientHost("clienthost")
            .setServerAssignorName("range")
            .setRackId("rackid")
            .setSubscribedTopicNames(List.of("foo"))
            .build();

        // The group and the member are created if they do not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("foo", member));
        assertEquals(member, context.groupMetadataManager.consumerGroup("foo").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayConsumerGroupMemberMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // ConsumerGroupMemberMetadata tombstone should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.consumerGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the ConsumerGroupMemberMetadata tombstone
        // should a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("bar"));
    }

    @Test
    public void testReplayConsumerGroupMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10));
        assertEquals(10, context.groupMetadataManager.consumerGroup("foo").groupEpoch());
    }

    @Test
    public void testReplayConsumerGroupMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupMetadata tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        Map<String, TopicMetadata> metadata = Map.of(
            "bar",
            new TopicMetadata(Uuid.randomUuid(), "bar", 10)
        );

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord("foo", metadata));
        assertEquals(metadata, context.groupMetadataManager.consumerGroup("foo").subscriptionMetadata());
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupPartitionMetadata tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMember() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        Map<Uuid, Set<Integer>> assignment = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 0, 1, 2)
        );

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("foo", "m1", assignment));
        assertEquals(assignment, context.groupMetadataManager.consumerGroup("foo").targetAssignment("m1").partitions());
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMemberTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupTargetAssignmentMember tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord("foo", 10));
        assertEquals(10, context.groupMetadataManager.consumerGroup("foo").assignmentEpoch());
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupTargetAssignmentMetadata tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignment() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(Uuid.randomUuid(), 0, 1, 2)))
            .build();

        // The group and the member are created if they do not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord("bar", member));
        assertEquals(member, context.groupMetadataManager.consumerGroup("bar").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignmentTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // ConsumerGroupCurrentMemberAssignment tombstone should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.consumerGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the ConsumerGroupCurrentMemberAssignment tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("bar"));
    }

    @Test
    public void testReplayStreamsGroupMemberMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setClientId("clientid")
            .setClientHost("clienthost")
            .setRackId("rackid")
            .setInstanceId("instanceid")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(10)
            .setProcessId("processid")
            .setUserEndpoint(new Endpoint().setHost("localhost").setPort(9999))
            .setClientTags(Collections.singletonMap("key", "value"))
            .build();

        // The group and the member are created if they do not exist.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord("foo", member));
        assertEquals(member, context.groupMetadataManager.streamsGroup("foo").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayStreamsGroupMemberMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // StreamsGroupMemberMetadata tombstone should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord("foo", 10));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.streamsGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the StreamsGroupMemberMetadata tombstone
        // should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("bar"));
    }

    @Test
    public void testReplayStreamsGroupMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord("foo", 10));
        assertEquals(10, context.groupMetadataManager.streamsGroup("foo").groupEpoch());
    }

    @Test
    public void testReplayStreamsGroupMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the StreamsGroupMetadata tombstone
        // should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupPartitionMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> metadata = Map.of(
            "bar",
            new org.apache.kafka.coordinator.group.streams.TopicMetadata(Uuid.randomUuid(), "bar", 10, Collections.emptyMap())
        );

        // The group is created if it does not exist.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupPartitionMetadataRecord("foo", metadata));
        assertEquals(metadata, context.groupMetadataManager.streamsGroup("foo").partitionMetadata());
    }

    @Test
    public void testReplayStreamsGroupPartitionMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the StreamsGroupPartitionMetadata tombstone
        // should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupPartitionMetadataTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMember() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        final Map<String, Set<Integer>> activeTasks = TaskAssignmentTestUtil.mkTasksPerSubtopology(
            TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2));
        final Map<String, Set<Integer>> standbyTasks = TaskAssignmentTestUtil.mkTasksPerSubtopology(
            TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5));
        final Map<String, Set<Integer>> warmupTasks = TaskAssignmentTestUtil.mkTasksPerSubtopology(
            TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord("foo", "m1",
            activeTasks,
            standbyTasks,
            warmupTasks
        ));
        assertEquals(activeTasks, context.groupMetadataManager.streamsGroup("foo").targetAssignment("m1").activeTasks());
        assertEquals(standbyTasks, context.groupMetadataManager.streamsGroup("foo").targetAssignment("m1").standbyTasks());
        assertEquals(warmupTasks, context.groupMetadataManager.streamsGroup("foo").targetAssignment("m1").warmupTasks());
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMemberTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the StreamsGroupTargetAssignmentMember tombstone
        // should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord("foo", 10));
        assertEquals(10, context.groupMetadataManager.streamsGroup("foo").assignmentEpoch());
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the StreamsGroupTargetAssignmentMetadata tombstone
        // should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignment() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
            .setAssignment(TaskAssignmentTestUtil.mkAssignment(
                TaskAssignmentTestUtil.mkTasksPerSubtopology(TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            ))
            .build();

        // The group and the member are created if they do not exist.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentRecord("bar", member));
        assertEquals(member, context.groupMetadataManager.streamsGroup("bar").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignmentTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // StreamsGroupCurrentMemberAssignment tombstone should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord("foo", 10));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.streamsGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the StreamsGroupCurrentMemberAssignment tombstone
        // should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("bar"));
    }

    @Test
    public void testReplayStreamsGroupTopology() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        StreamsGroupTopologyValue topology = new StreamsGroupTopologyValue()
            .setEpoch(12)
            .setSubtopologies(
                List.of(
                    new StreamsGroupTopologyValue.Subtopology()
                        .setSubtopologyId("subtopology-1")
                        .setSourceTopics(List.of("source-topic"))
                        .setRepartitionSinkTopics(List.of("sink-topic"))
                )
            );

        // The group and the topology are created if they do not exist.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTopologyRecord("bar", topology));
        assertEquals(topology.epoch(), context.groupMetadataManager.streamsGroup("bar").topology().topologyEpoch());
        assertEquals(topology.subtopologies().size(), context.groupMetadataManager.streamsGroup("bar").topology().subtopologies().size());
        assertEquals(
            topology.subtopologies().iterator().next(),
            context.groupMetadataManager.streamsGroup("bar").topology().subtopologies().values().iterator().next()
        );
    }

    @Test
    public void testReplayStreamsGroupTopologyTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // StreamsGroupTopology tombstone should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord("foo", 10));
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTopologyRecordTombstone("foo"));
        assertNull(context.groupMetadataManager.streamsGroup("foo").topology());

        // The group may not exist at all. Replaying the StreamsGroupTopology tombstone
        // should be a no-op.
        context.replay(CoordinatorStreamsRecordHelpers.newStreamsGroupTopologyRecordTombstone("bar"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("bar"));
    }


    @Test
    public void testConsumerGroupHeartbeatOnShareGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(MetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        assertThrows(GroupIdNotFoundException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId(groupId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testClassicGroupJoinOnShareGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(MetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(new JoinGroupRequestProtocolCollection(0))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testClassicGroupSyncOnShareGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(MetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        SyncGroupRequestData request = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withGenerationId(1)
            .withMemberId(memberId)
            .build();

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(request);

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testClassicGroupLeaveOnShareGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(MetadataImage.EMPTY)
                .withShareGroup(new ShareGroupBuilder(groupId, 1)
                    .withMember(new ShareGroupMember.Builder(memberId)
                        .setState(MemberState.STABLE)
                        .setMemberEpoch(1)
                        .setPreviousMemberEpoch(0)
                        .setClientId(DEFAULT_CLIENT_ID)
                        .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                        .setSubscribedTopicNames(List.of("foo"))
                        .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
                .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
            .setGroupId(groupId)
            .setMembers(List.of(
                new MemberIdentity()
                    .setMemberId(memberId)))));
    }

    @Test
    public void testConsumerGroupDescribeOnShareGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(MetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a consumer group.")
        );

        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.sendConsumerGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testShareGroupHeartbeatOnConsumerGroup() {
        String groupId = "group-foo";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));
    }

    @Test
    public void testShareGroupDescribeOnConsumerGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        int epoch = 10;
        String topicName = "topicName";
        ConsumerGroupMember.Builder memberBuilder = new ConsumerGroupMember.Builder(memberId)
            .setSubscribedTopicNames(List.of(topicName))
            .setServerAssignorName("assignorName");

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, epoch)
                .withMember(memberBuilder.build()))
            .build();

        List<ShareGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a share group.")
        );

        List<ShareGroupDescribeResponseData.DescribedGroup> actual = context.sendShareGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testReplayConsumerGroupRegularExpression() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ResolvedRegularExpression resolvedRegularExpression = new ResolvedRegularExpression(
            Set.of("abc", "abcd"),
            10L,
            12345L
        );

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
            "foo",
            "abc*",
            resolvedRegularExpression
        ));

        assertEquals(
            Optional.of(resolvedRegularExpression),
            context.groupMetadataManager.consumerGroup("foo").resolvedRegularExpression("abc*")
        );
    }

    @Test
    public void testReplayConsumerGroupRegularExpressionTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupRegularExpression tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone("foo", "abc*"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));

        // Otherwise, it should remove the regular expression.
        ResolvedRegularExpression resolvedRegularExpression = new ResolvedRegularExpression(
            Set.of("abc", "abcd"),
            10L,
            12345L
        );

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
            "foo",
            "abc*",
            resolvedRegularExpression
        ));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(
            "foo",
            "abc*"
        ));

        assertEquals(
            Optional.empty(),
            context.groupMetadataManager.consumerGroup("foo").resolvedRegularExpression("abc*")
        );
    }

    @Test
    public void testConsumerGroupMemberPicksUpExistingResolvedRegularExpression() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        ConsumerGroupPartitionAssignor assignor = mock(ConsumerGroupPartitionAssignor.class);
        when(assignor.name()).thenReturn("range");
        when(assignor.assign(any(), any())).thenAnswer(answer -> {
            GroupSpec spec = answer.getArgument(0);

            List.of(memberId1, memberId2).forEach(memberId ->
                assertEquals(
                    Collections.singleton(fooTopicId),
                    spec.memberSubscription(memberId).subscribedTopicIds(),
                    String.format("Member %s has unexpected subscribed topic ids", memberId)
                )
            );

            return new GroupAssignment(Map.of(
                memberId1, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0)
                )),
                memberId2, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)
                ))
            ));
        });

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Collections.singleton(fooTopicName),
                    100L,
                    12345L))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10))
            .build();

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(10000)
                .setSubscribedTopicRegex("foo*")
                .setTopicPartitions(Collections.emptyList()));

        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );
    }

    @Test
    public void testConsumerGroupMemberJoinsWithNewRegex() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .build(12345L))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 joins the consumer group with a new regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*")
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Execute pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(
            List.of(
                new MockCoordinatorExecutor.ExecutorResult<>(
                    groupId + "-regex",
                    new CoordinatorResult<>(List.of(
                        // The resolution of the new regex is persisted.
                        GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                            groupId,
                            "foo*",
                            new ResolvedRegularExpression(
                                Set.of("foo"),
                                12345L,
                                context.time.milliseconds()
                            )
                        ),
                        // The group epoch is bumped.
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
                    ))
                )
            ),
            tasks
        );
    }

    @Test
    public void testConsumerGroupMemberJoinsWithUpdatedRegex() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build(12345L))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        // Member 1 updates its new regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*|bar*")
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                    ))
                ),
            result.response()
        );

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The member subscription is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            // The previous regular expression is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*")
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Execute pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(1, tasks.size());

        MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord> task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key);
        assertRecordsEquals(
            List.of(
                // The resolution of the new regex is persisted.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    "foo*|bar*",
                    new ResolvedRegularExpression(
                        Set.of("foo", "bar"),
                        12345L,
                        context.time.milliseconds()
                    )
                ),
                // The updated subscription metadata.
                GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(
                    groupId,
                    Map.of(
                        "foo", new TopicMetadata(fooTopicId, fooTopicName, 6),
                        "bar", new TopicMetadata(barTopicId, barTopicName, 3)
                    )
                ),
                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
            ),
            task.result.records()
        );
    }

    @Test
    public void testConsumerGroupMemberJoinsWithRegexAndUpdatesItBeforeResolutionCompleted() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Collections.emptyMap()));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build(12345L))
            .build();

        // Member 1 joins.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*")
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The member subscription is created.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            // The group epoch is bumped.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1),
            // The target assignment is created.
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, Collections.emptyMap()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId, 1),
            // The member current state is created.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // The task is scheduled.
        assertTrue(context.executor.isScheduled(groupId + "-regex"));

        // The member updates its regex before the resolution of the previous one completes.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setSubscribedTopicRegex("foo*|bar*"));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .build();

        expectedRecords = List.of(
            // The member subscription is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            // The previous regex is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*")
        );

        assertRecordsEquals(expectedRecords, result.records());

        // The task is still scheduled.
        assertTrue(context.executor.isScheduled(groupId + "-regex"));
        assertEquals(1, context.executor.size());

        // Execute the pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(1, tasks.size());

        // The pending task was a no-op.
        MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord> task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key);
        assertRecordsEquals(Collections.emptyList(), task.result.records());

        // The member heartbeats again. It triggers a new resolution.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setSubscribedTopicRegex("foo*|bar*"));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertTrue(context.executor.isScheduled(groupId + "-regex"));
        assertEquals(1, context.executor.size());

        // Execute pending tasks.
        tasks = context.processTasks();
        assertEquals(1, tasks.size());

        task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key);
        assertRecordsEquals(
            List.of(
                // The resolution of the new regex is persisted.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    "foo*|bar*",
                    new ResolvedRegularExpression(
                        Set.of("foo", "bar"),
                        12345L,
                        context.time.milliseconds()
                    )
                ),
                // The updated subscription metadata.
                GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(
                    groupId,
                    Map.of(
                        "foo", new TopicMetadata(fooTopicId, fooTopicName, 6),
                        "bar", new TopicMetadata(barTopicId, barTopicName, 3)
                    )
                ),
                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2)
            ),
            task.result.records()
        );
    }

    @Test
    public void testConsumerGroupMemberJoinRefreshesExpiredRegexes() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid foooTopicId = Uuid.randomUuid();
        String foooTopicName = "fooo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Collections.emptyMap()));

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .build(1L);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(image)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(barTopicId, 0, 1, 2)))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withResolvedRegularExpression("bar*", new ResolvedRegularExpression(
                    Set.of(barTopicName), 0L, 0L))
                .withSubscriptionMetadata(Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Update metadata image.
        MetadataImage newImage = new MetadataImageBuilder(image)
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addTopic(foooTopicId, foooTopicName, 1)
            .build(2L);

        context.groupMetadataManager.onNewMetadataImage(
            newImage,
            new MetadataDelta(newImage)
        );

        // A member heartbeats.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10));

        // The task is NOT scheduled.
        assertFalse(context.executor.isScheduled(groupId + "-regex"));

        // Advance past the batching interval.
        context.sleep(11000L);

        // A member heartbeats.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10));

        // The task is scheduled.
        assertTrue(context.executor.isScheduled(groupId + "-regex"));

        // Execute the pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(1, tasks.size());

        // Execute pending tasks.
        MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord> task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key);

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                groupId,
                "foo*",
                new ResolvedRegularExpression(
                    Set.of(fooTopicName, foooTopicName),
                    2L,
                    context.time.milliseconds()
                )
            ),
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                groupId,
                "bar*",
                new ResolvedRegularExpression(
                    Set.of(barTopicName),
                    2L,
                    context.time.milliseconds()
                )
            ),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(
                groupId,
                Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 3),
                    foooTopicName, new TopicMetadata(foooTopicId, foooTopicName, 1)
                )
            ),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
        );

        assertUnorderedListEquals(expectedRecords.subList(0, 2), task.result.records().subList(0, 2));
        assertRecordsEquals(expectedRecords.subList(2, 4), task.result.records().subList(2, 4));
    }

    @Test
    public void testResolvedRegularExpressionsRemovedWhenMembersLeaveOrFenced() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Collections.emptyMap()));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build(1L))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(barTopicId, 0, 1, 2)))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withResolvedRegularExpression("bar*", new ResolvedRegularExpression(
                    Set.of(barTopicName), 0L, 0L))
                .withSubscriptionMetadata(Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Setup the timers.
        context.onLoaded();

        // Member 1 leaves the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(-1));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(-1),
            result.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId,
                Map.of(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3))
            ),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Member 2 is fenced due to reaching the session timeout.
        context.assertSessionTimeout(groupId, memberId2, 45000);
        List<ExpiredTimeout<Void, CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            Collections.singletonList(new ExpiredTimeout<Void, CoordinatorRecord>(
                groupSessionTimeoutKey(groupId, memberId2),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
                        GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "bar*"),
                        GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Collections.emptyMap()),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 12)
                    )
                )
            )),
            timeouts
        );
    }

    @Test
    public void testResolvedRegularExpressionsRemovedWhenConsumerMembersRemovedByAdminApi() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String memberId4 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Collections.emptyMap()));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, Collections.singletonList(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build(1L))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId1)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId2)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId3)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId3)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId4)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId4)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withResolvedRegularExpression("bar*", new ResolvedRegularExpression(
                    Set.of(barTopicName), 0L, 0L))
                .withSubscriptionMetadata(Map.of(
                    fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6),
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
                ))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Remove members.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> result = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId(groupId)
                .setMembers(List.of(
                    new MemberIdentity().setGroupInstanceId(memberId1),
                    new MemberIdentity().setGroupInstanceId(memberId2),
                    new MemberIdentity().setGroupInstanceId(memberId3)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(memberId1)
                        .setGroupInstanceId(memberId1),
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(memberId2)
                        .setGroupInstanceId(memberId2),
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(memberId3)
                        .setGroupInstanceId(memberId3)
                )),
            result.response()
        );

        assertRecordsEquals(
            List.of(
                // Remove member 1.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                // Remove member 2.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
                // Remove member 3.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId3),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId3),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId3),
                // Remove regex.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
                // Updated subscription metadata.
                GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord(groupId, Map.of(
                    barTopicName, new TopicMetadata(barTopicId, barTopicName, 3)
                )),
                // Bumped epoch.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11)
            ),
            result.records()
        );
    }

    private static void checkJoinGroupResponse(
        JoinGroupResponseData expectedResponse,
        JoinGroupResponseData actualResponse,
        ClassicGroup group,
        ClassicGroupState expectedState,
        Set<String> expectedGroupInstanceIds
    ) {
        assertEquals(expectedResponse, actualResponse);
        assertTrue(group.isInState(expectedState));

        Set<String> groupInstanceIds = actualResponse.members()
                                                     .stream()
                                                     .map(JoinGroupResponseMember::groupInstanceId)
                                                     .collect(Collectors.toSet());

        assertEquals(expectedGroupInstanceIds, groupInstanceIds);
    }

    private static List<JoinGroupResponseMember> toJoinResponseMembers(ClassicGroup group) {
        List<JoinGroupResponseMember> members = new ArrayList<>();
        String protocolName = group.protocolName().get();
        group.allMembers().forEach(member -> members.add(
            new JoinGroupResponseMember()
                .setMemberId(member.memberId())
                .setGroupInstanceId(member.groupInstanceId().orElse(""))
                .setMetadata(member.metadata(protocolName))
        ));

        return members;
    }

    private static List<String> verifyClassicGroupJoinResponses(
        List<GroupMetadataManagerTestContext.JoinResult> joinResults,
        int expectedSuccessCount,
        Errors expectedFailure
    ) {
        int successCount = 0;
        List<String> memberIds = new ArrayList<>();
        for (GroupMetadataManagerTestContext.JoinResult joinResult : joinResults) {
            if (!joinResult.joinFuture.isDone()) {
                fail("All responseFutures should be completed.");
            }
            try {
                JoinGroupResponseData joinResponse = joinResult.joinFuture.get();
                if (joinResponse.errorCode() == Errors.NONE.code()) {
                    successCount++;
                } else {
                    assertEquals(expectedFailure.code(), joinResponse.errorCode());
                }
                memberIds.add(joinResponse.memberId());
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }

        assertEquals(expectedSuccessCount, successCount);
        return memberIds;
    }
}
