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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Subtopology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Topology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.TasksTuple;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticHeartbeat;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticJoinHeartbeat;

/**
 * Compaction replay tests for the group coordinator.
 *
 * Tests check partition loading after compaction for non-trivial scenarios involving offset
 * commits and member joins/rebalances for an online classic -> consumer upgrade and an
 * offline classic -> streams upgrade. Both tests capture written records, compact the
 * resulting log, and replay records through a new group coordinator shard to verify loading.
 * Multiple contiguous log segments are tested, including prefix and mid-log windows.
 * 
 */
public class GroupMetadataManagerCompactionReplayTest {

    private static final String FOO_TOPIC_NAME = "foo";
    private static final String BAR_TOPIC_NAME = "bar";
    private static final String SUBTOPOLOGY_ID = "subtopology-1";

    private static final int MAX_RECONCILIATION_ROUNDS = 10;
    private static final long ASSIGNMENT_INTERVAL_ADVANCE_MS =
        GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT + 1;
    private static final int LONG_TIMEOUT_MS = 60000;

    private static final GroupCoordinatorConfig REPLAY_CONFIG = GroupCoordinatorConfig.fromProps(Map.of());
    private static final GroupCoordinatorMetrics REPLAY_METRICS = new GroupCoordinatorMetrics();

    private Uuid fooTopicId;
    private Uuid barTopicId;
    private CoordinatorMetadataImage metadataImage;
    private MockPartitionAssignor consumerAssignor;
    private MockTaskAssignor streamsAssignor;
    private GroupMetadataManagerTestContext context;

    @BeforeEach
    public void setUp() {
        fooTopicId = Uuid.randomUuid();
        barTopicId = Uuid.randomUuid();
        metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, FOO_TOPIC_NAME, 6)
            .addTopic(barTopicId, BAR_TOPIC_NAME, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();
        consumerAssignor = new MockPartitionAssignor("range");
        streamsAssignor = new MockTaskAssignor("sticky");
        context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(consumerAssignor))
            .withStreamsGroupTaskAssignors(List.of(streamsAssignor))
            .withMetadataImage(metadataImage)
            .build();
    }

    /**
     * Classic -> consumer group upgrade with offset commits. Related bugs: KAFKA-19862
     * 
     * Scenario:
     *  Classic group created
     *  Classic offset commit
     *  Classic group rebalance
     *  Member joins with consumer protocol
     *  Upgrades to consumer group
     *  Consumer group rebalance
     */
    @Test
    public void testClassicGroupUpgradeToConsumerGroupWithOffsetCommit() throws Exception {
        String groupId = "consumer-lifecycle-group";
        CapturedLog capturedLog = new CapturedLog();

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = createClassicGroupWithFirstMember(groupId, capturedLog);
        String classicMemberA = joinResponseA.memberId();
        syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5),
                new TopicPartition(BAR_TOPIC_NAME, 0),
                new TopicPartition(BAR_TOPIC_NAME, 1),
                new TopicPartition(BAR_TOPIC_NAME, 2))
        ), capturedLog);

        // Offset commit
        capturedLog.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 0, 10L));
        capturedLog.append(offsetCommitRecord(groupId, BAR_TOPIC_NAME, 0, 20L));

        // Member B joins with classic protocol, triggering rebalance
        String classicMemberB = context.sendClassicGroupJoin(
            classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true).joinFuture.get().memberId();
        context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberB), true);

        // Member A rejoins
        var rejoinA = context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberA), true);
        capturedLog.append(rejoinA.records);
        JoinGroupResponseData rejoinResponseA = rejoinA.joinFuture.get();
        syncClassicMember(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(BAR_TOPIC_NAME, 0)),
            classicMemberB, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5),
                new TopicPartition(BAR_TOPIC_NAME, 1),
                new TopicPartition(BAR_TOPIC_NAME, 2))
        ), capturedLog);
        syncClassicMember(groupId, classicMemberB, rejoinResponseA.generationId(), Map.of(), capturedLog);

        // Member C joins with consumer protocol, triggering an online classic -> consumer group upgrade.
        String memberC = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            classicMemberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        Map<String, ConsumerMemberState> members = new LinkedHashMap<>();
        joinConsumerMember(groupId, memberC, members, capturedLog);

        // Members A and B move onto the consumer protocol one at a time.
        String memberA = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        leaveClassicMember(groupId, classicMemberA, capturedLog);
        prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2)),
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0))));
        letAssignmentIntervalElapse(capturedLog);
        joinConsumerMember(groupId, memberA, members, capturedLog);
        completeConsumerGroupRebalance(groupId, members, capturedLog);

        String memberB = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        leaveClassicMember(groupId, classicMemberB, capturedLog);
        completeConsumerGroupRebalance(groupId, members, capturedLog);
        prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        letAssignmentIntervalElapse(capturedLog);
        joinConsumerMember(groupId, memberB, members, capturedLog);
        completeConsumerGroupRebalance(groupId, members, capturedLog);

        // Member D joins with consumer protocol, triggering a consumer group rebalance.
        String memberD = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 0), mkTopicAssignment(barTopicId, 0)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 1), mkTopicAssignment(barTopicId, 1)),
            memberD, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 2))));
        letAssignmentIntervalElapse(capturedLog);
        joinConsumerMember(groupId, memberD, members, capturedLog);
        completeConsumerGroupRebalance(groupId, members, capturedLog);

        // Group commits one more offset
        capturedLog.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 1, 30L));
        
        // Verify the partitions can be reloaded cleanly from log. 
        assertCompactedVariantsLoadCleanly(capturedLog);
    }

    /**
     * Classic -> streams upgrade with offset commits. Related bugs: KAFKA-19862, KAFKA-20254
     * 
     * Scenario:
     *  Classic group created
     *  Classic offset commit
     *  Classic group rebalance
     *  Group upgrades offline to streams protocol
     *  Members join/leave and group rebalances accordingly
     */
    @Test
    public void testClassicGroupMigratedToStreamsGroupLoadsCleanlyUnderCompaction() throws Exception {
        String groupId = "streams-lifecycle-group";
        CapturedLog capturedLog = new CapturedLog();

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = createClassicGroupWithFirstMember(groupId, capturedLog);
        String classicMemberA = joinResponseA.memberId();
        syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ), capturedLog);

        // Offset commit
        capturedLog.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 0, 10L));
        capturedLog.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 1, 20L));

        // Member B joins with classic protocol, triggering rebalance
        String classicMemberB = context.sendClassicGroupJoin(
            classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true).joinFuture.get().memberId();
        context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberB), true);

        // Member A rejoins
        var rejoinA = context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberA), true);
        capturedLog.append(rejoinA.records);
        JoinGroupResponseData rejoinResponseA = rejoinA.joinFuture.get();
        syncClassicMember(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2)),
            classicMemberB, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ), capturedLog);
        syncClassicMember(groupId, classicMemberB, rejoinResponseA.generationId(), Map.of(), capturedLog);

        // Group is shut down for offline upgrade to streams
        leaveClassicMember(groupId, classicMemberA, capturedLog);
        leaveClassicMember(groupId, classicMemberB, capturedLog);

        // Group restarts with streams protocol. The leftover classic group is tombstoned.
        String streamsMemberA = Uuid.randomUuid().toString();
        streamsAssignor.prepareGroupAssignment(Map.of(streamsMemberA, tasks(0, 1, 2, 3, 4, 5)));
        Map<String, StreamsMemberState> members = new LinkedHashMap<>();
        joinStreamsMember(groupId, streamsMemberA, "process-a", members, capturedLog);
        completeStreamsGroupRebalance(groupId, members, capturedLog);

        // Member B joins and group rebalances.
        String streamsMemberB = Uuid.randomUuid().toString();
        streamsAssignor.prepareGroupAssignment(Map.of(
            streamsMemberA, tasks(0, 1, 2),
            streamsMemberB, tasks(3, 4, 5)));
        letAssignmentIntervalElapse(capturedLog);
        joinStreamsMember(groupId, streamsMemberB, "process-b", members, capturedLog);
        completeStreamsGroupRebalance(groupId, members, capturedLog);

        // Member C joins and the group rebalances.
        String streamsMemberC = Uuid.randomUuid().toString();
        streamsAssignor.prepareGroupAssignment(Map.of(
            streamsMemberA, tasks(0, 1),
            streamsMemberB, tasks(2, 3),
            streamsMemberC, tasks(4, 5)));
        letAssignmentIntervalElapse(capturedLog);
        joinStreamsMember(groupId, streamsMemberC, "process-c", members, capturedLog);
        completeStreamsGroupRebalance(groupId, members, capturedLog);

        // Member A leaves and the group rebalances.
        streamsAssignor.prepareGroupAssignment(Map.of(
            streamsMemberB, tasks(0, 1, 2),
            streamsMemberC, tasks(3, 4, 5)));
        letAssignmentIntervalElapse(capturedLog);
        leaveStreamsMember(groupId, streamsMemberA, members, capturedLog);
        completeStreamsGroupRebalance(groupId, members, capturedLog);

        // Member B leaves and group rebalances (all tasks now owned by member C).
        streamsAssignor.prepareGroupAssignment(Map.of(streamsMemberC, tasks(0, 1, 2, 3, 4, 5)));
        letAssignmentIntervalElapse(capturedLog);
        leaveStreamsMember(groupId, streamsMemberB, members, capturedLog);
        completeStreamsGroupRebalance(groupId, members, capturedLog);

        capturedLog.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 2, 30L));
        
        // Verify partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly(capturedLog);
    }

    // ------------------------------------------------------------------------------------------
    // Scenario helpers.
    // ------------------------------------------------------------------------------------------

    /**
     * A classic join request using the consumer embedded protocol.
     */
    private JoinGroupRequestData classicJoinRequest(String groupId, String memberId) {
        return new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId)
            .withProtocolType("consumer")
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(FOO_TOPIC_NAME, BAR_TOPIC_NAME), List.of()))
            .withRebalanceTimeoutMs(LONG_TIMEOUT_MS)
            .withSessionTimeoutMs(LONG_TIMEOUT_MS)
            .build();
    }

    /**
     * Creates a classic group when the first member joins.
     */
    private JoinGroupResponseData createClassicGroupWithFirstMember(
        String groupId,
        CapturedLog capturedLog
    ) throws Exception {
        var firstJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true);
        capturedLog.append(firstJoin.records);
        firstJoin.appendFuture.complete(null);
        String memberId = firstJoin.joinFuture.get().memberId();

        var secondJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, memberId), true);
        capturedLog.append(secondJoin.records);
        // The first generation only forms once the initial rebalance delay has elapsed.
        sleepCapturing(context.classicGroupInitialRebalanceDelayMs, capturedLog);
        return secondJoin.joinFuture.get();
    }

    /**
     * Advances the clock, capturing whatever the timeouts that fired wrote.
     */
    private void sleepCapturing(long durationMs, CapturedLog capturedLog) {
        context.sleep(durationMs).forEach(timeout -> capturedLog.append(timeout.result().records()));
    }

    /**
     * Advances the clock past the assignment interval so that the next heartbeat is allowed to run the
     * assignor and move partitions or tasks between members.
     */
    private void letAssignmentIntervalElapse(CapturedLog capturedLog) {
        sleepCapturing(ASSIGNMENT_INTERVAL_ADVANCE_MS, capturedLog);
    }

    /**
     * Sends a member's SyncGroup with {@code assignments}.
     */
    private void syncClassicMember(
        String groupId,
        String memberId,
        int generationId,
        Map<String, List<TopicPartition>> assignments,
        CapturedLog capturedLog
    ) {
        var syncResult = context.sendClassicGroupSync(new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId)
            .withGenerationId(generationId)
            .withAssignment(assignments.entrySet().stream()
                .map(entry -> new SyncGroupRequestData.SyncGroupRequestAssignment()
                    .setMemberId(entry.getKey())
                    .setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(
                        new ConsumerPartitionAssignor.Assignment(entry.getValue())))))
                .collect(Collectors.toList()))
            .build());
        syncResult.appendFuture.complete(null);
        capturedLog.append(syncResult.records);
    }

    /**
     * Removes a classic member.
     */
    private void leaveClassicMember(String groupId, String memberId, CapturedLog capturedLog) {
        var result = context.sendClassicGroupLeave(new LeaveGroupRequestData()
            .setGroupId(groupId)
            .setMembers(List.of(new LeaveGroupRequestData.MemberIdentity().setMemberId(memberId))));
        result.records().forEach(context::replay);
        capturedLog.append(result.records());
    }

    private void prepareConsumerAssignment(Map<String, Map<Uuid, Set<Integer>>> assignments) {
        consumerAssignor.prepareGroupAssignment(new GroupAssignment(assignments.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new MemberAssignmentImpl(entry.getValue())))));
    }

    /**
     * The epoch and owned partitions of a consumer group member
     */
    private static final class ConsumerMemberState {
        int memberEpoch;
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedPartitions = List.of();

        ConsumerMemberState(ConsumerGroupHeartbeatResponseData response) {
            update(response);
        }

        void update(ConsumerGroupHeartbeatResponseData response) {
            memberEpoch = response.memberEpoch();
            if (response.assignment() != null) {
                ownedPartitions = response.assignment().topicPartitions().stream()
                    .map(topicPartitions -> new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicPartitions.topicId())
                        .setPartitions(topicPartitions.partitions()))
                    .collect(Collectors.toList());
            }
        }
    }

    /**
     * Joins a member with the consumer protocol.
     */
    private void joinConsumerMember(
        String groupId,
        String memberId,
        Map<String, ConsumerMemberState> members,
        CapturedLog capturedLog
    ) {
        var result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(LONG_TIMEOUT_MS)
                .setSubscribedTopicNames(List.of(FOO_TOPIC_NAME, BAR_TOPIC_NAME))
                .setTopicPartitions(List.of())
        );
        capturedLog.append(result.records());
        members.put(memberId, new ConsumerMemberState(result.response()));
    }

    /**
     * Heartbeats every member until a whole round writes no records, which drives a pending rebalance
     * through revocation to completion.
     */
    private void heartbeatAndRebalance(
        String protocol,
        Set<String> memberIds,
        CapturedLog capturedLog,
        Function<String, List<CoordinatorRecord>> heartbeat
    ) {
        for (int round = 0; round < MAX_RECONCILIATION_ROUNDS; round++) {
            letAssignmentIntervalElapse(capturedLog);
            boolean progressed = false;
            for (String memberId : memberIds) {
                List<CoordinatorRecord> records = heartbeat.apply(memberId);
                if (!records.isEmpty()) {
                    progressed = true;
                    capturedLog.append(records);
                }
            }
            if (!progressed) {
                return;
            }
        }
        throw new AssertionError("The " + protocol + " group did not stop writing records after "
            + MAX_RECONCILIATION_ROUNDS + " rounds of heartbeats.");
    }

    private void completeConsumerGroupRebalance(String groupId, Map<String, ConsumerMemberState> members, CapturedLog capturedLog) {
        heartbeatAndRebalance("consumer", members.keySet(), capturedLog, memberId -> {
            ConsumerMemberState state = members.get(memberId);
            var result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(state.memberEpoch)
                .setTopicPartitions(state.ownedPartitions));
            state.update(result.response());
            return result.records();
        });
    }

    /**
     * Removes a streams member. The streams counterpart of {@link #leaveClassicMember}.
     */
    private void leaveStreamsMember(
        String groupId,
        String memberId,
        Map<String, StreamsMemberState> members,
        CapturedLog capturedLog
    ) {
        capturedLog.append(context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, null, LEAVE_GROUP_MEMBER_EPOCH)).records());
        members.remove(memberId);
    }

    private TasksTuple tasks(Integer... partitions) {
        return TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(SUBTOPOLOGY_ID, partitions));
    }

    /**
     * The streams counterpart of {@link ConsumerMemberState}.
     */
    private static final class StreamsMemberState {
        int memberEpoch;
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks = List.of();
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks = List.of();
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedWarmupTasks = List.of();

        StreamsMemberState(StreamsGroupHeartbeatResponseData response) {
            update(response);
        }

        void update(StreamsGroupHeartbeatResponseData response) {
            memberEpoch = response.memberEpoch();
            if (response.activeTasks() != null) {
                ownedActiveTasks = ownedTasks(response.activeTasks());
            }
            if (response.standbyTasks() != null) {
                ownedStandbyTasks = ownedTasks(response.standbyTasks());
            }
            if (response.warmupTasks() != null) {
                ownedWarmupTasks = ownedTasks(response.warmupTasks());
            }
        }

        private static List<StreamsGroupHeartbeatRequestData.TaskIds> ownedTasks(
            List<StreamsGroupHeartbeatResponseData.TaskIds> tasks
        ) {
            return tasks.stream()
                .map(taskIds -> new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(taskIds.subtopologyId())
                    .setPartitions(taskIds.partitions()))
                .collect(Collectors.toList());
        }
    }

    /**
     * The streams counterpart of {@link #joinConsumerMember}.
     */
    private void joinStreamsMember(
        String groupId,
        String memberId,
        String processId,
        Map<String, StreamsMemberState> members,
        CapturedLog capturedLog
    ) {
        var result = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, memberId, null, processId)
                .setRebalanceTimeoutMs(LONG_TIMEOUT_MS)
                .setTopology(new Topology().setSubtopologies(List.of(
                    new Subtopology().setSubtopologyId(SUBTOPOLOGY_ID).setSourceTopics(List.of(FOO_TOPIC_NAME)))))
        );
        capturedLog.append(result.records());
        members.put(memberId, new StreamsMemberState(result.response().data()));
    }

    /**
     * The streams counterpart of {@link #completeConsumerGroupRebalance}.
     */
    private void completeStreamsGroupRebalance(String groupId, Map<String, StreamsMemberState> members, CapturedLog capturedLog) {
        heartbeatAndRebalance("streams", members.keySet(), capturedLog, memberId -> {
            StreamsMemberState state = members.get(memberId);
            var result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, null, state.memberEpoch)
                    .setActiveTasks(state.ownedActiveTasks)
                    .setStandbyTasks(state.ownedStandbyTasks)
                    .setWarmupTasks(state.ownedWarmupTasks));
            state.update(result.response().data());
            return result.records();
        });
    }

    private CoordinatorRecord offsetCommitRecord(String groupId, String topic, int partition, long offset) {
        return GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            groupId,
            topic,
            partition,
            new OffsetAndMetadata(
                offset,
                OptionalInt.empty(),
                "",
                context.time.milliseconds(),
                OptionalLong.empty(),
                topic.equals(FOO_TOPIC_NAME) ? fooTopicId : barTopicId));
    }

    // ------------------------------------------------------------------------------------------
    // Compaction model.
    // ------------------------------------------------------------------------------------------

    /**
     * The records a scenario wrote, kept grouped by the write that produced them. The records of one
     * write are appended as a single batch, which is the unit compaction cannot split.
     */
    private static final class CapturedLog {
        private final List<List<CoordinatorRecord>> batches = new ArrayList<>();

        void append(List<CoordinatorRecord> batch) {
            if (!batch.isEmpty()) {
                batches.add(List.copyOf(batch));
            }
        }

        void append(CoordinatorRecord record) {
            append(List.of(record));
        }

        List<CoordinatorRecord> records() {
            return batches.stream().flatMap(List::stream).toList();
        }

        /**
         * The positions in {@link #records()} at which a batch starts, plus the length of the log:
         * the boundaries a cleaning window may fall on.
         */
        List<Integer> batchBoundaries() {
            List<Integer> boundaries = new ArrayList<>();
            int position = 0;
            for (List<CoordinatorRecord> batch : batches) {
                boundaries.add(position);
                position += batch.size();
            }
            boundaries.add(position);
            return boundaries;
        }
    }

    /**
     * One compacted variant of a capturedLogd log: the records a load observes, the window that was cleaned
     * to produce them, and the positions in the original log the cleaner removed.
     */
    private record CompactedVariant(
        List<CoordinatorRecord> records,
        int from,
        int to,
        List<Integer> removedPositions
    ) {
        @Override
        public String toString() {
            return removedPositions.isEmpty()
                ? "the uncompacted log"
                : "the log with window [" + from + ", " + to + ") cleaned, removing positions " + removedPositions;
        }
    }

    /**
     * Replays every compacted variant (each contiguous subset of record batches) of {@code capturedLog} 
     * through a fresh coordinator and asserts that each one loads without throwing.
     */
    private void assertCompactedVariantsLoadCleanly(CapturedLog capturedLog) {
        List<CoordinatorRecord> log = capturedLog.records();

        Set<ApiMessage> laterKeys = new HashSet<>();
        Set<Integer> compactable = new HashSet<>();
        for (int position = log.size() - 1; position >= 0; position--) {
            CoordinatorRecord record = log.get(position);
            if (record.value() == null || laterKeys.contains(record.key())) {
                compactable.add(position);
            }
            laterKeys.add(record.key());
        }

        List<Integer> boundaries = capturedLog.batchBoundaries();

        assertLoadsCleanly(compact(log, compactable, 0, 0));

        for (int firstBatch = 0; firstBatch < boundaries.size() - 1; firstBatch++) {
            for (int lastBatch = firstBatch + 1; lastBatch < boundaries.size(); lastBatch++) {
                assertLoadsCleanly(compact(log, compactable, boundaries.get(firstBatch), boundaries.get(lastBatch)));
            }
        }
    }

    /**
     * Replays {@code variant} through a real {@link GroupCoordinatorShard} over a fresh coordinator.
     */
    private void assertLoadsCleanly(CompactedVariant variant) {
        GroupMetadataManagerTestContext replayContext = 
            new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new MockPartitionAssignor("range")))
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(metadataImage)
                .build();
        LogContext logContext = new LogContext();
        GroupCoordinatorShard shard = new GroupCoordinatorShard(
            logContext,
            replayContext.groupMetadataManager,
            new OffsetMetadataManager.Builder()
                .withLogContext(logContext)
                .withTime(replayContext.time)
                .withSnapshotRegistry(replayContext.snapshotRegistry)
                .withGroupMetadataManager(replayContext.groupMetadataManager)
                .withGroupCoordinatorConfig(REPLAY_CONFIG)
                .withGroupCoordinatorMetricsShard(replayContext.metrics)
                .build(),
            replayContext.time,
            replayContext.timer,
            REPLAY_CONFIG,
            REPLAY_METRICS,
            replayContext.metrics
        );

        List<CoordinatorRecord> records = variant.records();
        int offset = 0;
        try {
            for (; offset < records.size(); offset++) {
                shard.replay(offset, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, records.get(offset));
            }
        } catch (Throwable t) {
            throw new AssertionError(
                "Replaying " + variant + " failed to load.\n"
                    + "  failed on surviving record " + offset + " " + records.get(offset).key() + "\n"
                    + "  survivingRecords=" + records.stream()
                        .map(record -> record.key().getClass().getSimpleName()
                            + (record.value() == null ? "(tombstone)" : ""))
                        .collect(Collectors.joining(", ")),
                t
            );
        }
    }

    /**
     * Removes every compactable record in {@code log[from..to)}. Models a contiguous
     * stretch of compacted log records. 
     */
    private static CompactedVariant compact(
        List<CoordinatorRecord> log,
        Set<Integer> compactable,
        int from,
        int to
    ) {
        Set<ApiMessage> survivingKeys = new HashSet<>();
        List<CoordinatorRecord> compacted = new ArrayList<>(log.size());
        List<Integer> removed = new ArrayList<>();
        for (int position = 0; position < log.size(); position++) {
            CoordinatorRecord record = log.get(position);
            boolean cleaned = position >= from && position < to && compactable.contains(position);
            boolean isRetainedTombstone = record.value() == null && survivingKeys.contains(record.key());
            if (!cleaned || isRetainedTombstone) {
                compacted.add(record);
                survivingKeys.add(record.key());
            } else {
                removed.add(position);
            }
        }
        return new CompactedVariant(compacted, from, to, removed);
    }
}
