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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.TasksTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.max;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticHeartbeat;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticJoinHeartbeat;

/**
 * Drives group coordinator scenarios and records the resulting log for {@link
 * GroupCoordinatorShardCompactionReplayTest}. Each request helper runs an operation against a live
 * {@link GroupMetadataManagerTestContext} and appends whatever records it produced, so scenarios
 * read as a sequence of coordinator operations rather than of log bookkeeping.
 *
 * <p>The records a scenario wrote are kept grouped by the write that produced them. The records of
 * one write are appended as a single batch.
 */
final class CompactionReplayTestContext {

    static final String FOO_TOPIC_NAME = "foo";
    static final String BAR_TOPIC_NAME = "bar";
    static final String SUBTOPOLOGY_ID = "subtopology-1";

    private static final int MAX_RECONCILIATION_ROUNDS = 10;
    private static final int LONG_TIMEOUT_MS = 60000;

    private final GroupMetadataManagerTestContext context;
    private final MockPartitionAssignor consumerAssignor;
    private final MockTaskAssignor streamsAssignor;
    private final CoordinatorMetadataImage metadataImage;

    private final List<List<CoordinatorRecord>> batches = new ArrayList<>();

    CompactionReplayTestContext(
        GroupMetadataManagerTestContext context,
        MockPartitionAssignor consumerAssignor,
        MockTaskAssignor streamsAssignor,
        CoordinatorMetadataImage metadataImage
    ) {
        this.context = context;
        this.consumerAssignor = consumerAssignor;
        this.streamsAssignor = streamsAssignor;
        this.metadataImage = metadataImage;
    }

    private void append(List<CoordinatorRecord> batch) {
        if (!batch.isEmpty()) {
            batches.add(List.copyOf(batch));
        }
    }

    private void append(CoordinatorRecord record) {
        append(List.of(record));
    }

    /**
     * The records every scenario step wrote, in order.
     */
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

    // Request helpers.

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
    JoinGroupResponseData joinFirstClassicMember(String groupId) throws Exception {
        var firstJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true);
        append(firstJoin.records);
        firstJoin.appendFuture.complete(null);
        String memberId = firstJoin.joinFuture.get().memberId();

        var secondJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, memberId), true);
        append(secondJoin.records);
        secondJoin.appendFuture.complete(null);
        // The first generation only forms once the initial rebalance delay has elapsed.
        sleepCapturing(context.classicGroupInitialRebalanceDelayMs);
        return secondJoin.joinFuture.get();
    }

    /**
     * Joins a new member to an existing classic group, triggering a rebalance. The member first joins
     * with an unknown id to be assigned one, then rejoins with it. Returns the assigned member id.
     */
    String joinClassicMember(String groupId) throws Exception {
        var firstJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true);
        append(firstJoin.records);
        String memberId = firstJoin.joinFuture.get().memberId();

        var secondJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, memberId), true);
        append(secondJoin.records);
        return memberId;
    }

    /**
     * Rejoins an existing classic member, capturing the rebalance it triggers.
     */
    JoinGroupResponseData rejoinClassicMember(String groupId, String memberId) throws Exception {
        var rejoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, memberId), true);
        append(rejoin.records);
        return rejoin.joinFuture.get();
    }

    /**
     * Advances the clock, capturing whatever the timeouts that fired wrote.
     */
    private void sleepCapturing(long durationMs) {
        context.sleep(durationMs).forEach(timeout -> append(timeout.result().records()));
    }

    /**
     * Advances the clock past the assignment interval so that the next heartbeat is allowed to run the
     * assignor and move partitions or tasks between members.
     */
    void waitForAssignmentInterval() {
        long assignmentIntervalAdvanceMs =
            max(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT,
                GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT
            ) + 1;
        sleepCapturing(assignmentIntervalAdvanceMs);
    }

    /**
     * Sends a member's SyncGroup with {@code assignments}.
     */
    void syncClassicMember(
        String groupId,
        String memberId,
        int generationId,
        Map<String, List<TopicPartition>> assignments
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
        append(syncResult.records);
    }

    /**
     * Removes a classic member.
     */
    void leaveClassicMember(String groupId, String memberId) {
        var result = context.sendClassicGroupLeave(new LeaveGroupRequestData()
            .setGroupId(groupId)
            .setMembers(List.of(new LeaveGroupRequestData.MemberIdentity().setMemberId(memberId))));
        result.records().forEach(context::replay);
        append(result.records());
    }

    void prepareConsumerAssignment(Map<String, Map<Uuid, Set<Integer>>> assignments) {
        consumerAssignor.prepareGroupAssignment(new GroupAssignment(assignments.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new MemberAssignmentImpl(entry.getValue())))));
    }

    void prepareStreamsAssignment(Map<String, TasksTuple> assignments) {
        streamsAssignor.prepareGroupAssignment(assignments);
    }

    /**
     * Joins a member with the consumer protocol.
     */
    void joinConsumerMember(
        String groupId,
        String memberId,
        Map<String, ConsumerMemberState> members
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
        append(result.records());
        members.put(memberId, new ConsumerMemberState(result.response()));
    }

    void completeConsumerGroupRebalance(String groupId, Map<String, ConsumerMemberState> members) {
        heartbeatAndRebalance("consumer", members.keySet(), memberId -> {
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

    TasksTuple tasks(Integer... partitions) {
        return TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(SUBTOPOLOGY_ID, partitions));
    }

    /**
     * Joins a member with the streams protocol. The streams counterpart of {@link #joinConsumerMember}.
     */
    void joinStreamsMember(
        String groupId,
        String memberId,
        String processId,
        Map<String, StreamsMemberState> members
    ) {
        var result = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, memberId, null, processId)
                .setRebalanceTimeoutMs(LONG_TIMEOUT_MS)
                .setTopology(new Topology().setSubtopologies(List.of(
                    new Subtopology().setSubtopologyId(SUBTOPOLOGY_ID).setSourceTopics(List.of(FOO_TOPIC_NAME)))))
        );
        append(result.records());
        members.put(memberId, new StreamsMemberState(result.response().data()));
    }

    /**
     * Removes a streams member. The streams counterpart of {@link #leaveClassicMember}.
     */
    void leaveStreamsMember(
        String groupId,
        String memberId,
        Map<String, StreamsMemberState> members
    ) {
        append(context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, null, LEAVE_GROUP_MEMBER_EPOCH)).records());
        members.remove(memberId);
    }

    void completeStreamsGroupRebalance(String groupId, Map<String, StreamsMemberState> members) {
        heartbeatAndRebalance("streams", members.keySet(), memberId -> {
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

    /**
     * Heartbeats every member until a whole round writes no records, which drives a pending rebalance
     * through revocation to completion.
     */
    private void heartbeatAndRebalance(
        String protocol,
        Set<String> memberIds,
        Function<String, List<CoordinatorRecord>> heartbeat
    ) {
        for (int round = 0; round < MAX_RECONCILIATION_ROUNDS; round++) {
            waitForAssignmentInterval();
            boolean progressed = false;
            for (String memberId : memberIds) {
                List<CoordinatorRecord> records = heartbeat.apply(memberId);
                if (!records.isEmpty()) {
                    progressed = true;
                    append(records);
                }
            }
            if (!progressed) {
                return;
            }
        }
        throw new AssertionError("The " + protocol + " group did not stop writing records after "
            + MAX_RECONCILIATION_ROUNDS + " rounds of heartbeats.");
    }

    /**
     * Commits an offset for the group.
     */
    void commitOffset(String groupId, String topic, int partition, long offset) {
        append(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            groupId,
            topic,
            partition,
            new OffsetAndMetadata(
                offset,
                OptionalInt.empty(),
                "",
                context.time.milliseconds(),
                OptionalLong.empty(),
                metadataImage.topicMetadata(topic).orElseThrow().id())));
    }

    /**
     * The epoch and owned partitions of a consumer group member.
     */
    static final class ConsumerMemberState {
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
     * The streams counterpart of {@link ConsumerMemberState}.
     */
    static final class StreamsMemberState {
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
}
