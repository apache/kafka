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
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Subtopology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Topology;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newGroupMetadataRecord;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Compaction replay tests for the group coordinator (KCOORD-1410).
 *
 * <p>These tests model Kafka log compaction of the {@code __consumer_offsets} topic as a
 * clean-prefix / dirty-suffix split at every batch boundary, then replay every resulting compacted
 * variant through a fresh group coordinator to verify it still loads cleanly.
 *
 * <p>Each test exercises the real coordinator through a common use case, collecting the records it
 * writes (grouped by write), and hands them to {@link #assertCompactedVariantsLoadCleanly}.
 */
public class GroupMetadataManagerCompactionReplayTest {

    private Uuid fooTopicId;
    private Uuid barTopicId;
    private CoordinatorMetadataImage metadataImage;
    private MockPartitionAssignor assignor;
    private MockTaskAssignor streamsAssignor;
    private GroupMetadataManagerTestContext context;

    @BeforeEach
    public void setUp() {
        fooTopicId = Uuid.randomUuid();
        barTopicId = Uuid.randomUuid();
        metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 6)
            .addTopic(barTopicId, "bar", 3)
            .addRacks()
            .buildCoordinatorMetadataImage();
        assignor = new MockPartitionAssignor("range");
        streamsAssignor = new MockTaskAssignor("sticky");
        context = newContext(assignor, streamsAssignor);
    }

    /**
     * A coordinator over the shared metadata image, wired with both a consumer-group and a
     * streams-group assignor (and the classic-to-consumer migration policy, a no-op for groups that
     * never start out classic) so a single setup serves every scenario.
     */
    private GroupMetadataManagerTestContext newContext(MockPartitionAssignor assignor, MockTaskAssignor streamsAssignor) {
        return new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withStreamsGroupTaskAssignors(List.of(streamsAssignor))
            .withMetadataImage(metadataImage)
            .build();
    }

    /**
     * A fresh, empty coordinator (same metadata image and config) used to replay compacted variants.
     * Replay never invokes the assignors, so throwaway ones are fine.
     */
    private GroupMetadataManagerTestContext freshContext() {
        return newContext(new MockPartitionAssignor("range"), new MockTaskAssignor("sticky"));
    }

    /**
     * Two members join a consumer group and rebalance, which exercises group creation, subscription
     * metadata, target assignment, and current assignment records (and the churn between them).
     */
    @Test
    public void testRebalanceLoadsCleanlyUnderCompaction() {
        String groupId = "rebalance-group";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        List<List<CoordinatorRecord>> batches = new ArrayList<>();

        // Member 1 joins and gets all partitions.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
        )));
        var member1Join = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setTopicPartitions(List.of()));
        batches.add(member1Join.records());
        int member1Epoch = member1Join.response().memberEpoch();

        // Member 2 joins, triggering a rebalance that splits partitions across both members.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0))),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 1, 2)))
        )));
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setTopicPartitions(List.of())).records());

        // Member 1 heartbeats with its current epoch to reconcile onto its new (smaller)
        // assignment, completing the rebalance.
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(member1Epoch)).records());

        assertCompactedVariantsLoadCleanly(batches, this::freshContext);
    }

    /**
     * A single member changes its subscription from {@code [foo]} to {@code [foo, bar]}, exercising
     * subscription-metadata updates, a group-epoch bump, and a follow-up reassignment.
     */
    @Test
    public void testSubscriptionChangeLoadsCleanlyUnderCompaction() {
        String groupId = "subscription-change-group";
        String memberId = Uuid.randomUuid().toString();
        List<List<CoordinatorRecord>> batches = new ArrayList<>();

        // Initial subscription to foo only.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)))
        )));
        var join = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setTopicPartitions(List.of()));
        batches.add(join.records());

        // Subscription expands to foo and bar.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
        )));
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(join.response().memberEpoch())
            .setSubscribedTopicNames(List.of("foo", "bar"))).records());

        assertCompactedVariantsLoadCleanly(batches, this::freshContext);
    }

    /**
     * A member joins and then leaves the group. The leave writes tombstones for the member's
     * subscription, target assignment, and current assignment (and, as the last member leaves, the
     * group itself), which is exactly the state the {@code dropTombstones} sweep variants stress.
     */
    @Test
    public void testMemberLeaveLoadsCleanlyUnderCompaction() {
        String groupId = "member-leave-group";
        String memberId = Uuid.randomUuid().toString();
        List<List<CoordinatorRecord>> batches = new ArrayList<>();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)))
        )));
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setTopicPartitions(List.of())).records());

        // Member leaves the group.
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)).records());

        assertCompactedVariantsLoadCleanly(batches, this::freshContext);
    }

    /**
     * A static member (with a group instance id) leaves with the static-leave epoch and a new member
     * id rejoins under the same instance id, replacing the static member. This exercises the
     * static-member replacement records, which carry tombstones for the old member id.
     */
    @Test
    public void testStaticMemberRejoinLoadsCleanlyUnderCompaction() {
        String groupId = "static-rejoin-group";
        String instanceId = "instance-1";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        List<List<CoordinatorRecord>> batches = new ArrayList<>();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)))
        )));
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setTopicPartitions(List.of())).records());

        // Static member temporarily leaves (static-leave epoch keeps its assignment reserved).
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setInstanceId(instanceId)
            .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)).records());

        // A new member id rejoins under the same instance id, replacing the old static member.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId2, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)))
        )));
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setTopicPartitions(List.of())).records());

        assertCompactedVariantsLoadCleanly(batches, this::freshContext);
    }

    /**
     * A classic group with one member is upgraded to the consumer protocol when a second member
     * joins with the new protocol (migration policy UPGRADE). The captured log starts with the
     * classic {@code GroupMetadata} record and is followed by the upgrade batch, which tombstones
     * the classic group and writes the full set of consumer-group records. This is the scenario the
     * ticket calls out by name and the one most prone to load failures, because compaction can drop
     * the classic group record independently of the consumer-group records that replace it.
     */
    @Test
    public void testClassicToConsumerUpgradeLoadsCleanlyUnderCompaction() {
        String groupId = "upgrade-group";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        List<List<CoordinatorRecord>> batches = new ArrayList<>();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of("foo"),
                null,
                List.of(new TopicPartition("foo", 0)))))));
        Map<String, byte[]> assignments = Map.of(memberId1,
            Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(
                List.of(new TopicPartition("foo", 0))))));

        // Build a stable classic group with member 1 and persist its GroupMetadata record.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(new ClassicGroupMember(
            memberId1, Optional.empty(), "client-id", "client-host", 10000, 5000, "consumer",
            protocols, assignments.get(memberId1)));
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        CoordinatorRecord classicGroupRecord = newGroupMetadataRecord(group, assignments);
        context.replay(classicGroupRecord);
        context.commit();
        batches.add(List.of(classicGroupRecord));

        // Member 2 joins with the new protocol, triggering the upgrade.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0))),
            memberId2, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2)))
        )));
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(5000)
            .setServerAssignor("range")
            .setSubscribedTopicNames(List.of("foo"))
            .setTopicPartitions(List.of())).records());

        assertCompactedVariantsLoadCleanly(batches, this::freshContext);
    }

    /**
     * KCOORD-1232: a consumer group commits an offset and then rewrites all of its group records (by
     * changing its subscription), so after full compaction the offset-commit record sorts before the
     * surviving group records. On replay the offset commit creates a simple classic group, and the
     * consumer group records must still load on top of it rather than failing with "not a consumer
     * group".
     */
    @Test
    public void testConsumerGroupWithOffsetCommitLoadsCleanlyUnderCompaction() {
        String groupId = "offset-commit-consumer-group";
        String memberId = Uuid.randomUuid().toString();
        List<List<CoordinatorRecord>> batches = new ArrayList<>();

        // Member joins subscribed to foo.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)))
        )));
        var join = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setTopicPartitions(List.of()));
        batches.add(join.records());

        // Commit an offset for foo-0.
        batches.add(List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            groupId, "foo", 0,
            new OffsetAndMetadata(10L, OptionalInt.empty(), "", context.time.milliseconds(), OptionalLong.empty(), fooTopicId))));

        // Member leaves, deleting the (now empty) group.
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)).records());

        // Member rejoins, recreating the group. Every group record is now rewritten at an offset
        // after the offset commit, so full compaction sorts the offset commit first and replaying it
        // creates a simple classic group before the consumer group records are replayed.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)))
        )));
        batches.add(context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setTopicPartitions(List.of())).records());

        assertCompactedVariantsLoadCleanly(batches, this::freshContext);
    }

    /**
     * KSTREAMS-8756: the streams-group counterpart of the scenario above. A streams group commits an
     * offset and is then deleted and recreated, so after full compaction the offset-commit record
     * sorts before the streams group records. On replay the offset commit creates a simple classic
     * group, and the streams group records must still load on top of it.
     */
    @Test
    public void testStreamsGroupWithOffsetCommitLoadsCleanlyUnderCompaction() {
        String groupId = "offset-commit-streams-group";
        String memberId = Uuid.randomUuid().toString();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId("subtopology-1").setSourceTopics(List.of("foo"))));
        List<List<CoordinatorRecord>> batches = new ArrayList<>();

        // Member joins, creating the streams group.
        streamsAssignor.prepareGroupAssignment(Map.of(
            memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2))));
        batches.add(context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1500)
            .setTopology(topology)
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of())).records());

        // Commit an offset for foo-0.
        batches.add(List.of(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
            groupId, "foo", 0,
            new OffsetAndMetadata(10L, OptionalInt.empty(), "", context.time.milliseconds(), OptionalLong.empty(), fooTopicId))));

        // The group is deleted (e.g. via an admin DeleteGroups), tombstoning all of its records
        // including the topology. (A streams group is not auto-deleted when its last member leaves.)
        List<CoordinatorRecord> deletion = new ArrayList<>();
        context.groupMetadataManager.createGroupTombstoneRecordsAndCancelTimers(groupId, deletion);
        deletion.forEach(context::replay);
        batches.add(deletion);

        // Member rejoins, recreating the group so every group record (including the topology) is
        // rewritten after the offset commit; full compaction then sorts the offset commit first and
        // replaying it creates a simple classic group before the streams group records are replayed.
        streamsAssignor.prepareGroupAssignment(Map.of(
            memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2))));
        batches.add(context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1500)
            .setTopology(topology)
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of())).records());

        assertCompactedVariantsLoadCleanly(batches, this::freshContext);
    }

    /**
     * Replays every compacted variant of {@code batches} through a fresh coordinator and asserts it
     * loads without throwing.
     *
     * <p>{@code batches} is the records the coordinator wrote, grouped by write (one
     * {@code CoordinatorResult} per batch). Log compaction cleans at segment granularity and the
     * records produced by a single write never span a segment boundary, so they are always
     * compacted as an atomic unit. The clean/dirty boundary can therefore only fall <em>between</em>
     * batches, never inside one, which is what the sweep below models.
     */
    static void assertCompactedVariantsLoadCleanly(
        List<List<CoordinatorRecord>> batches,
        Supplier<GroupMetadataManagerTestContext> freshContext
    ) {
        List<CoordinatorRecord> log = new ArrayList<>();
        // Offsets where the clean/dirty boundary may fall: 0, then the cumulative size after each
        // non-empty batch (which includes the full log length).
        List<Integer> batchBoundaries = new ArrayList<>();
        batchBoundaries.add(0);
        for (List<CoordinatorRecord> batch : batches) {
            if (batch.isEmpty()) {
                continue;
            }
            log.addAll(batch);
            batchBoundaries.add(log.size());
        }

        assertFalse(log.isEmpty(), "Scenario produced no records; the capture is broken.");

        for (int boundaryN : batchBoundaries) {
            for (boolean dropTombstones : new boolean[] {false, true}) {
                List<CoordinatorRecord> variant = compact(log, boundaryN, dropTombstones);
                GroupMetadataManagerTestContext replayContext = freshContext.get();
                try {
                    replayAll(replayContext, variant);
                } catch (Throwable t) {
                    throw new AssertionError(
                        "Replaying a compacted variant failed to load.\n"
                            + "  boundaryN=" + boundaryN + " of " + log.size() + "\n"
                            + "  dropTombstones=" + dropTombstones + "\n"
                            + "  compactedKeys=" + describeKeys(variant),
                        t
                    );
                }
            }
        }
    }

    /**
     * Replays a compacted record list into a fresh coordinator, mirroring the partition load path.
     * Offset-commit records are routed to an {@link OffsetMetadataManager} that shares the same
     * {@link GroupMetadataManager}; replaying an offset commit for an unknown group creates a
     * "simple" classic group to hold the offsets, exactly as it does during a real load.
     */
    private static void replayAll(GroupMetadataManagerTestContext context, List<CoordinatorRecord> records) {
        OffsetMetadataManager offsetMetadataManager = new OffsetMetadataManager.Builder()
            .withTime(context.time)
            .withSnapshotRegistry(context.snapshotRegistry)
            .withGroupMetadataManager(context.groupMetadataManager)
            .withGroupCoordinatorConfig(GroupCoordinatorConfig.fromProps(Map.of()))
            .withGroupCoordinatorMetricsShard(context.metrics)
            .build();

        long offset = 0;
        for (CoordinatorRecord record : records) {
            if (CoordinatorRecordType.fromId(record.key().apiKey()) == CoordinatorRecordType.OFFSET_COMMIT) {
                offsetMetadataManager.replay(
                    offset,
                    RecordBatch.NO_PRODUCER_ID,
                    (OffsetCommitKey) record.key(),
                    record.value() == null ? null : (OffsetCommitValue) record.value().message()
                );
            } else {
                context.replay(record);
            }
            offset++;
        }
    }

    static String describeKeys(List<CoordinatorRecord> records) {
        return records.stream()
            .map(record -> record.key().getClass().getSimpleName() + (record.value() == null ? "(tombstone)" : ""))
            .collect(Collectors.joining(", "));
    }

    @Test
    public void testCompactKeepsLastValuePerKeyInPrefix() {
        CoordinatorRecord epochV1 = newConsumerGroupEpochRecord("group", 1, 0L);
        CoordinatorRecord epochV2 = newConsumerGroupEpochRecord("group", 2, 0L);

        // Whole log is in the cleaned prefix; both records share the same key, so only the
        // last value survives.
        assertEquals(
            List.of(epochV2),
            compact(List.of(epochV1, epochV2), 2, false)
        );
    }

    @Test
    public void testCompactLeavesDirtySuffixUntouched() {
        CoordinatorRecord epochV1 = newConsumerGroupEpochRecord("group", 1, 0L);
        CoordinatorRecord epochV2 = newConsumerGroupEpochRecord("group", 2, 0L);

        // Boundary 1: only epochV1 is in the cleaned prefix; epochV2 is in the untouched dirty
        // suffix, so the duplicate key is NOT collapsed.
        assertEquals(
            List.of(epochV1, epochV2),
            compact(List.of(epochV1, epochV2), 1, false)
        );
    }

    @Test
    public void testCompactRetainsTombstoneInPrefixWhenNotDropped() {
        CoordinatorRecord epoch = newConsumerGroupEpochRecord("group", 1, 0L);
        CoordinatorRecord tombstone = newConsumerGroupEpochTombstoneRecord("group");

        // Last record for the key is the tombstone; with dropTombstones=false it survives.
        assertEquals(
            List.of(tombstone),
            compact(List.of(epoch, tombstone), 2, false)
        );
    }

    @Test
    public void testCompactDropsTombstoneInPrefixWhenDropped() {
        CoordinatorRecord epoch = newConsumerGroupEpochRecord("group", 1, 0L);
        CoordinatorRecord tombstone = newConsumerGroupEpochTombstoneRecord("group");

        // Tombstone surviving in the cleaned prefix is removed once delete.retention.ms expires.
        assertEquals(
            List.of(),
            compact(List.of(epoch, tombstone), 2, true)
        );
    }

    @Test
    public void testCompactOrdersSurvivorsByLastOccurrence() {
        CoordinatorRecord groupA1 = newConsumerGroupEpochRecord("groupA", 1, 0L);
        CoordinatorRecord groupB1 = newConsumerGroupEpochRecord("groupB", 1, 0L);
        CoordinatorRecord groupA2 = newConsumerGroupEpochRecord("groupA", 2, 0L);

        // groupA's last occurrence is after groupB's, so groupA's surviving record sorts last.
        assertEquals(
            List.of(groupB1, groupA2),
            compact(List.of(groupA1, groupB1, groupA2), 3, false)
        );
    }

    /**
     * Compacts {@code log[0..boundaryN)} (keeping only the last record per key) and leaves
     * {@code log[boundaryN..)} untouched. When {@code dropTombstones} is set, tombstone records
     * surviving in the cleaned prefix are additionally removed, modelling expiry of
     * {@code delete.retention.ms}.
     */
    static List<CoordinatorRecord> compact(
        List<CoordinatorRecord> log,
        int boundaryN,
        boolean dropTombstones
    ) {
        // Cleaned prefix: keep only the last record per key. Re-inserting an existing key moves it
        // to the end, so surviving records end up ordered by their last occurrence, matching how
        // compaction retains each key's record at its latest offset.
        LinkedHashMap<ApiMessage, CoordinatorRecord> cleaned = new LinkedHashMap<>();
        for (CoordinatorRecord record : log.subList(0, boundaryN)) {
            cleaned.remove(record.key());
            cleaned.put(record.key(), record);
        }

        List<CoordinatorRecord> compacted = new ArrayList<>();
        for (CoordinatorRecord record : cleaned.values()) {
            // A tombstone surviving in the cleaned prefix is eventually removed once
            // delete.retention.ms expires.
            if (dropTombstones && record.value() == null) {
                continue;
            }
            compacted.add(record);
        }

        // Dirty suffix is untouched.
        compacted.addAll(log.subList(boundaryN, log.size()));
        return compacted;
    }
}
