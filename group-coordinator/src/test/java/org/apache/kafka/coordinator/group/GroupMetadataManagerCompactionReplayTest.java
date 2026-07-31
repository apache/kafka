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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Compaction replay tests for the group coordinator (KCOORD-1410).
 *
 * <p>A group coordinator that loads {@code __consumer_offsets} while the log cleaner is running can
 * observe a log from which records have been removed. These tests capture the records a real
 * coordinator writes over the lifetime of a group, remove records from the captured log the way
 * compaction would, and replay every resulting variant through a fresh coordinator to verify it
 * still loads.
 *
 * <h2>Compaction model</h2>
 *
 * <p>A record is <em>compactable</em> when the cleaner is allowed to remove it: it is superseded by a
 * later record with the same key, or it is a tombstone (which is itself removed once
 * {@code delete.retention.ms} expires). Every other record holds the latest value for its key and
 * must be retained.
 *
 * <p>Each variant picks a contiguous stretch of the log and removes every compactable record in it,
 * modelling a load that observes that stretch in its cleaned form. The sweep covers every such
 * stretch, so it includes both the fully compacted prefix and windows that start in the middle of
 * the log. Two constraints keep the variants to logs a load can actually observe:
 *
 * <ul>
 *   <li>The window starts and ends on a batch boundary. The records of a single write are appended
 *       as one batch, a batch never spans a segment, and the cleaner works a segment at a time, so
 *       the records of one write always share the same fate.</li>
 *   <li>A tombstone is only removed when no earlier record with the same key survives. Dropping an
 *       aged-out tombstone happens in a later cleaner pass than the one that collapsed its key, so
 *       a surviving predecessor cannot outlive it.</li>
 * </ul>
 *
 * <h2>Bugs covered</h2>
 *
 * <p>A window that starts at the beginning of the log leaves the surviving offset-commit records
 * ahead of the surviving group records. Replaying an offset commit for an unknown group creates a
 * simple classic group, so the group records that follow have to load on top of it — the
 * KSTREAMS-8756 (KAFKA-20254) streams failure and its consumer-group counterpart.
 *
 * <p>A window that starts in the middle of the log drops the record that unassigns a partition or
 * task from a member while keeping the earlier record that assigned it. That is the shape behind
 * KCOORD-1232 (KAFKA-19862): the load has to tolerate a partition still being owned at an unexpected
 * epoch instead of failing.
 */
public class GroupMetadataManagerCompactionReplayTest {

    private static final String FOO_TOPIC_NAME = "foo";
    private static final String BAR_TOPIC_NAME = "bar";
    private static final String SUBTOPOLOGY_ID = "subtopology-1";

    /**
     * A rebalance takes a bounded number of heartbeat rounds to reconcile; more than this means the
     * scenario is looping rather than converging.
     */
    private static final int MAX_RECONCILIATION_ROUNDS = 10;

    /**
     * The assignor only runs once per assignment interval, so the clock has to advance past it before
     * the coordinator will compute a new target assignment.
     */
    private static final long ASSIGNMENT_INTERVAL_ADVANCE_MS =
        GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_DEFAULT + 1;

    /**
     * Long enough that no member is fenced while the scenario advances the clock between rebalances.
     */
    private static final int LONG_TIMEOUT_MS = 60000;

    /**
     * Parsed once and shared by every replay context: building it per variant dominates the runtime
     * of the sweep.
     */
    private static final GroupCoordinatorConfig REPLAY_CONFIG = GroupCoordinatorConfig.fromProps(Map.of());

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
            .addTopic(fooTopicId, FOO_TOPIC_NAME, 6)
            .addTopic(barTopicId, BAR_TOPIC_NAME, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();
        assignor = new MockPartitionAssignor("range");
        streamsAssignor = new MockTaskAssignor("sticky");
        context = newContext(assignor, streamsAssignor);
    }

    /**
     * A coordinator over the shared metadata image, wired with both a consumer-group and a
     * streams-group assignor and with online classic-to-consumer upgrades enabled.
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
     * A new coordinator (same metadata image and config) used to replay a compacted variant.
     */
    private GroupMetadataManagerTestContext freshContext() {
        return newContext(new MockPartitionAssignor("range"), new MockTaskAssignor("sticky"));
    }

    /**
     * The full life of a consumer group that started out on the classic protocol: it is created,
     * commits offsets, rebalances as a classic group, is upgraded online when a member joins with
     * the consumer protocol, has its remaining classic members roll onto the consumer protocol one
     * at a time, and rebalances again as a consumer group.
     *
     * <p>The log therefore mixes classic {@code GroupMetadata} records and their tombstone, offset
     * commits and the full set of consumer-group records, and it moves partitions between members
     * several times.
     */
    @Test
    public void testClassicGroupUpgradedToConsumerGroupLoadsCleanlyUnderCompaction() throws Exception {
        assertCompactedVariantsLoadCleanly(captureClassicGroupUpgradedToConsumerGroup(), this::freshContext);
    }

    private CapturedLog captureClassicGroupUpgradedToConsumerGroup() throws Exception {
        String groupId = "consumer-lifecycle-group";
        CapturedLog capture = new CapturedLog();

        // A classic group is created: its first member joins and syncs the assignment it computed,
        // which persists the group's first GroupMetadata record.
        JoinGroupResponseData joinResponseA = createClassicGroupWithFirstMember(groupId, capture);
        String classicMemberA = joinResponseA.memberId();
        syncClassicLeader(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
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
        ), capture);

        // The group commits offsets. These records are never rewritten afterwards, so they end up
        // ahead of the surviving group records in a compacted log.
        capture.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 0, 10L));
        capture.append(offsetCommitRecord(groupId, BAR_TOPIC_NAME, 0, 20L));

        // A second member joins, which starts a rebalance that waits for the other members to rejoin.
        // A dynamic member only learns its member id on the first round trip, so it joins twice.
        String classicMemberB = context.sendClassicGroupJoin(
            classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true).joinFuture.get().memberId();
        var classicJoinB = context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberB), true);
        assertFalse(classicJoinB.joinFuture.isDone(), "The rebalance should wait for the other members to rejoin.");

        // Member A rejoins, so the join phase completes and the next generation is persisted.
        var rejoinA = context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberA), true);
        capture.append(rejoinA.records);
        JoinGroupResponseData rejoinResponseA = rejoinA.joinFuture.get();
        syncClassicLeader(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
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
        ), capture);
        var followerSync = context.sendClassicGroupSync(new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(classicMemberB)
            .withGenerationId(rejoinResponseA.generationId())
            .build());
        followerSync.appendFuture.complete(null);
        capture.append(followerSync.records);

        // A third member joins with the consumer protocol, upgrading the group online. This
        // tombstones the classic GroupMetadata record and writes the full set of consumer-group
        // records for all three members.
        String memberC = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            classicMemberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        var upgrade = context.consumerGroupHeartbeat(consumerJoinRequest(groupId, memberC));
        capture.append(upgrade.records());
        Map<String, ConsumerMemberState> members = new LinkedHashMap<>();
        members.put(memberC, new ConsumerMemberState(upgrade.response()));

        // The two classic members roll onto the consumer protocol one at a time: each leaves the
        // group and comes back with a new member id, moving partitions between members twice.
        String memberA = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        leaveClassicMember(groupId, classicMemberA, capture);
        prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2)),
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0))));
        letAssignmentIntervalElapse(capture);
        var joinA = context.consumerGroupHeartbeat(consumerJoinRequest(groupId, memberA));
        capture.append(joinA.records());
        members.put(memberA, new ConsumerMemberState(joinA.response()));
        reconcileConsumerGroup(groupId, members, capture);

        String memberB = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        leaveClassicMember(groupId, classicMemberB, capture);
        reconcileConsumerGroup(groupId, members, capture);
        prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        letAssignmentIntervalElapse(capture);
        var joinB = context.consumerGroupHeartbeat(consumerJoinRequest(groupId, memberB));
        capture.append(joinB.records());
        members.put(memberB, new ConsumerMemberState(joinB.response()));
        reconcileConsumerGroup(groupId, members, capture);

        // The group is scaled out once more, which shuffles the partitions across four members. Every
        // partition has now had several owners, so the last records in the log unassign partitions
        // that earlier records assigned elsewhere.
        String memberD = Uuid.randomUuid().toString();
        prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 0), mkTopicAssignment(barTopicId, 0)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 1), mkTopicAssignment(barTopicId, 1)),
            memberD, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 2))));
        letAssignmentIntervalElapse(capture);
        var joinD = context.consumerGroupHeartbeat(consumerJoinRequest(groupId, memberD));
        capture.append(joinD.records());
        members.put(memberD, new ConsumerMemberState(joinD.response()));
        reconcileConsumerGroup(groupId, members, capture);

        capture.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 1, 30L));
        return capture;
    }

    /**
     * The full life of a Kafka Streams application that is migrated offline from the classic protocol
     * to the streams protocol: it runs as a classic group, commits offsets, rebalances, shuts down,
     * and comes back on the streams protocol under the same group id, which deletes the leftover
     * empty classic group and creates a streams group in its place. A second streams instance then
     * joins and the tasks are redistributed.
     *
     * <p>This is the shape behind KSTREAMS-8756 (KAFKA-20254): the offset commits outlive the classic
     * {@code GroupMetadata} record and its tombstone, so a compacted log replays them before any
     * streams-group record.
     */
    @Test
    public void testClassicGroupMigratedToStreamsGroupLoadsCleanlyUnderCompaction() throws Exception {
        assertCompactedVariantsLoadCleanly(captureClassicGroupMigratedToStreamsGroup(), this::freshContext);
    }

    private CapturedLog captureClassicGroupMigratedToStreamsGroup() throws Exception {
        String groupId = "streams-lifecycle-group";
        CapturedLog capture = new CapturedLog();

        // The application first runs on the classic protocol: a classic group is created and its
        // first member syncs an assignment.
        JoinGroupResponseData joinResponseA = createClassicGroupWithFirstMember(groupId, capture);
        String classicMemberA = joinResponseA.memberId();
        syncClassicLeader(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ), capture);

        // It commits offsets, which survive the migration and are never rewritten afterwards.
        capture.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 0, 10L));
        capture.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 1, 20L));

        // A second instance starts, so the classic group rebalances. As above, the new member joins
        // twice and the rebalance completes once member A rejoins.
        String classicMemberB = context.sendClassicGroupJoin(
            classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true).joinFuture.get().memberId();
        var joinB = context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberB), true);
        assertFalse(joinB.joinFuture.isDone(), "The rebalance should wait for the other members to rejoin.");

        var rejoinA = context.sendClassicGroupJoin(classicJoinRequest(groupId, classicMemberA), true);
        capture.append(rejoinA.records);
        JoinGroupResponseData rejoinResponseA = rejoinA.joinFuture.get();
        syncClassicLeader(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2)),
            classicMemberB, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ), capture);
        var followerSync = context.sendClassicGroupSync(new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(classicMemberB)
            .withGenerationId(rejoinResponseA.generationId())
            .build());
        followerSync.appendFuture.complete(null);
        capture.append(followerSync.records);

        // The application is shut down for the migration. Both members leave, leaving behind an
        // empty classic group that still holds the committed offsets.
        leaveClassicMember(groupId, classicMemberA, capture);
        leaveClassicMember(groupId, classicMemberB, capture);

        // It restarts on the streams protocol under the same group id. The leftover empty classic
        // group is tombstoned and a streams group is created in its place.
        String streamsMemberA = Uuid.randomUuid().toString();
        streamsAssignor.prepareGroupAssignment(Map.of(streamsMemberA, tasks(0, 1, 2, 3, 4, 5)));
        var streamsJoinA = context.streamsGroupHeartbeat(streamsJoinRequest(groupId, streamsMemberA, "process-a"));
        capture.append(streamsJoinA.records());
        Map<String, StreamsMemberState> members = new LinkedHashMap<>();
        members.put(streamsMemberA, new StreamsMemberState(streamsJoinA.response().data()));
        reconcileStreamsGroup(groupId, members, capture);

        // A second streams instance joins and the active tasks are redistributed, so the log
        // unassigns tasks from one member and assigns them to the other.
        String streamsMemberB = Uuid.randomUuid().toString();
        streamsAssignor.prepareGroupAssignment(Map.of(
            streamsMemberA, tasks(0, 1, 2),
            streamsMemberB, tasks(3, 4, 5)));
        letAssignmentIntervalElapse(capture);
        var streamsJoinB = context.streamsGroupHeartbeat(streamsJoinRequest(groupId, streamsMemberB, "process-b"));
        capture.append(streamsJoinB.records());
        members.put(streamsMemberB, new StreamsMemberState(streamsJoinB.response().data()));
        reconcileStreamsGroup(groupId, members, capture);

        // A third instance joins and the tasks are spread across all three processes, moving some of
        // them to their second owner.
        String streamsMemberC = Uuid.randomUuid().toString();
        streamsAssignor.prepareGroupAssignment(Map.of(
            streamsMemberA, tasks(0, 1),
            streamsMemberB, tasks(2, 3),
            streamsMemberC, tasks(4, 5)));
        letAssignmentIntervalElapse(capture);
        var streamsJoinC = context.streamsGroupHeartbeat(streamsJoinRequest(groupId, streamsMemberC, "process-c"));
        capture.append(streamsJoinC.records());
        members.put(streamsMemberC, new StreamsMemberState(streamsJoinC.response().data()));
        reconcileStreamsGroup(groupId, members, capture);

        // The first instance leaves and its tasks move on again.
        streamsAssignor.prepareGroupAssignment(Map.of(
            streamsMemberB, tasks(0, 1, 2),
            streamsMemberC, tasks(3, 4, 5)));
        letAssignmentIntervalElapse(capture);
        capture.append(context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(streamsMemberA)
            .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)).records());
        members.remove(streamsMemberA);
        reconcileStreamsGroup(groupId, members, capture);

        // The second instance leaves too, so the last one ends up owning every task, which means
        // every task has been owned by at least two different processes over the log.
        streamsAssignor.prepareGroupAssignment(Map.of(streamsMemberC, tasks(0, 1, 2, 3, 4, 5)));
        letAssignmentIntervalElapse(capture);
        capture.append(context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(streamsMemberB)
            .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)).records());
        members.remove(streamsMemberB);
        reconcileStreamsGroup(groupId, members, capture);

        capture.append(offsetCommitRecord(groupId, FOO_TOPIC_NAME, 2, 30L));
        return capture;
    }

    // ------------------------------------------------------------------------------------------
    // Scenario helpers.
    // ------------------------------------------------------------------------------------------

    /**
     * A classic join request using the consumer embedded protocol, which is what an online upgrade to
     * the consumer protocol requires.
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
     * Creates a classic group by joining its first member. A dynamic member only learns its member id
     * on the first round trip, so it joins twice, and creating the group commits an empty
     * {@code GroupMetadata} record before the first generation is formed.
     */
    private JoinGroupResponseData createClassicGroupWithFirstMember(
        String groupId,
        CapturedLog capture
    ) throws Exception {
        var firstJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, UNKNOWN_MEMBER_ID), true);
        capture.append(firstJoin.records);
        firstJoin.appendFuture.complete(null);
        String memberId = firstJoin.joinFuture.get().memberId();

        var secondJoin = context.sendClassicGroupJoin(classicJoinRequest(groupId, memberId), true);
        capture.append(secondJoin.records);
        // The first generation only forms once the initial rebalance delay has elapsed.
        context.sleep(context.classicGroupInitialRebalanceDelayMs)
            .forEach(timeout -> capture.append(timeout.result().records()));
        return secondJoin.joinFuture.get();
    }

    /**
     * Advances the clock past the assignment interval so that the next heartbeat is allowed to run the
     * assignor and move partitions or tasks between members, capturing whatever the timeouts that
     * fired wrote.
     */
    private void letAssignmentIntervalElapse(CapturedLog capture) {
        context.sleep(ASSIGNMENT_INTERVAL_ADVANCE_MS)
            .forEach(timeout -> capture.append(timeout.result().records()));
    }

    /**
     * Sends the leader's SyncGroup with {@code assignments}, which persists the group's
     * {@code GroupMetadata} record.
     */
    private void syncClassicLeader(
        String groupId,
        String leaderId,
        int generationId,
        Map<String, List<TopicPartition>> assignments,
        CapturedLog capture
    ) {
        var syncResult = context.sendClassicGroupSync(new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(leaderId)
            .withGenerationId(generationId)
            .withAssignment(assignments.entrySet().stream()
                .map(entry -> new SyncGroupRequestData.SyncGroupRequestAssignment()
                    .setMemberId(entry.getKey())
                    .setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(
                        new ConsumerPartitionAssignor.Assignment(entry.getValue())))))
                .collect(Collectors.toList()))
            .build());
        syncResult.appendFuture.complete(null);
        capture.append(syncResult.records);
    }

    /**
     * Removes a classic member, as happens when an instance shuts down.
     */
    private void leaveClassicMember(String groupId, String memberId, CapturedLog capture) {
        var result = context.sendClassicGroupLeave(new LeaveGroupRequestData()
            .setGroupId(groupId)
            .setMembers(List.of(new LeaveGroupRequestData.MemberIdentity().setMemberId(memberId))));
        result.records().forEach(context::replay);
        capture.append(result.records());
    }

    private ConsumerGroupHeartbeatRequestData consumerJoinRequest(String groupId, String memberId) {
        return new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(0)
            .setServerAssignor("range")
            .setRebalanceTimeoutMs(LONG_TIMEOUT_MS)
            .setSubscribedTopicNames(List.of(FOO_TOPIC_NAME, BAR_TOPIC_NAME))
            .setTopicPartitions(List.of());
    }

    private void prepareConsumerAssignment(Map<String, Map<Uuid, Set<Integer>>> assignments) {
        assignor.prepareGroupAssignment(new GroupAssignment(assignments.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new MemberAssignmentImpl(entry.getValue())))));
    }

    /**
     * What a consumer-group member reports on its next heartbeat: the epoch the coordinator last gave
     * it and the partitions it currently owns. A member only revokes a partition once it stops
     * reporting it as owned, so echoing the assignment back is what lets a rebalance finish.
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
     * Heartbeats each member until the group stops writing records, which drives a pending rebalance
     * through revocation to completion.
     */
    private void reconcileConsumerGroup(String groupId, Map<String, ConsumerMemberState> members, CapturedLog capture) {
        for (int round = 0; round < MAX_RECONCILIATION_ROUNDS; round++) {
            letAssignmentIntervalElapse(capture);
            boolean progressed = false;
            for (Map.Entry<String, ConsumerMemberState> entry : members.entrySet()) {
                ConsumerMemberState state = entry.getValue();
                var result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(entry.getKey())
                    .setMemberEpoch(state.memberEpoch)
                    .setTopicPartitions(state.ownedPartitions));
                state.update(result.response());
                if (!result.records().isEmpty()) {
                    progressed = true;
                    capture.append(result.records());
                }
            }
            if (!progressed) {
                return;
            }
        }
        throw new AssertionError("The consumer group did not stop writing records after "
            + MAX_RECONCILIATION_ROUNDS + " rounds of heartbeats.");
    }

    /**
     * A streams join request. Each instance runs in its own process, and task ownership is tracked per
     * process id, so the ids have to be distinct for tasks to actually change owner.
     */
    private StreamsGroupHeartbeatRequestData streamsJoinRequest(String groupId, String memberId, String processId) {
        return new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setProcessId(processId)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(LONG_TIMEOUT_MS)
            .setTopology(new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId(SUBTOPOLOGY_ID).setSourceTopics(List.of(FOO_TOPIC_NAME)))))
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of());
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
     * The streams counterpart of {@link #reconcileConsumerGroup}.
     */
    private void reconcileStreamsGroup(String groupId, Map<String, StreamsMemberState> members, CapturedLog capture) {
        for (int round = 0; round < MAX_RECONCILIATION_ROUNDS; round++) {
            letAssignmentIntervalElapse(capture);
            boolean progressed = false;
            for (Map.Entry<String, StreamsMemberState> entry : members.entrySet()) {
                StreamsMemberState state = entry.getValue();
                var result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(entry.getKey())
                    .setMemberEpoch(state.memberEpoch)
                    .setActiveTasks(state.ownedActiveTasks)
                    .setStandbyTasks(state.ownedStandbyTasks)
                    .setWarmupTasks(state.ownedWarmupTasks));
                state.update(result.response().data());
                if (!result.records().isEmpty()) {
                    progressed = true;
                    capture.append(result.records());
                }
            }
            if (!progressed) {
                return;
            }
        }
        throw new AssertionError("The streams group did not stop writing records after "
            + MAX_RECONCILIATION_ROUNDS + " rounds of heartbeats.");
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
            batches.add(List.of(record));
        }

        List<CoordinatorRecord> records() {
            return batches.stream().flatMap(List::stream).collect(Collectors.toList());
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
     * Replays every compacted variant of {@code capture} through a fresh coordinator and asserts that
     * each one loads without throwing.
     */
    private static void assertCompactedVariantsLoadCleanly(
        CapturedLog capture,
        Supplier<GroupMetadataManagerTestContext> freshContext
    ) {
        List<CoordinatorRecord> log = capture.records();
        assertFalse(log.isEmpty(), "Scenario produced no records; the capture is broken.");
        Set<Integer> compactable = compactablePositions(log);
        List<Integer> boundaries = capture.batchBoundaries();

        // The uncompacted log, which every window below is a variant of.
        assertLoadsCleanly(log, log, freshContext, 0, 0);

        for (int from = 0; from < boundaries.size() - 1; from++) {
            for (int to = from + 1; to < boundaries.size(); to++) {
                assertLoadsCleanly(
                    compact(log, compactable, boundaries.get(from), boundaries.get(to)),
                    log,
                    freshContext,
                    boundaries.get(from),
                    boundaries.get(to)
                );
            }
        }
    }

    /**
     * Replays {@code variant} through a real {@link GroupCoordinatorShard} over a fresh coordinator,
     * which is the dispatch a broker uses to load a {@code __consumer_offsets} partition. Going
     * through the production path is what makes the offset commits matter: replaying one for a group
     * that does not exist yet creates a "simple" classic group in the shared
     * {@link GroupMetadataManager}, which the group records that follow then have to load on top of.
     */
    private static void assertLoadsCleanly(
        List<CoordinatorRecord> variant,
        List<CoordinatorRecord> log,
        Supplier<GroupMetadataManagerTestContext> freshContext,
        int from,
        int to
    ) {
        GroupMetadataManagerTestContext context = freshContext.get();
        GroupCoordinatorShard shard = new GroupCoordinatorShard(
            new LogContext(),
            context.groupMetadataManager,
            new OffsetMetadataManager.Builder()
                .withTime(context.time)
                .withSnapshotRegistry(context.snapshotRegistry)
                .withGroupMetadataManager(context.groupMetadataManager)
                .withGroupCoordinatorConfig(REPLAY_CONFIG)
                .withGroupCoordinatorMetricsShard(context.metrics)
                .build(),
            context.time,
            context.timer,
            REPLAY_CONFIG,
            new GroupCoordinatorMetrics(),
            context.metrics
        );

        int offset = 0;
        try {
            for (; offset < variant.size(); offset++) {
                shard.replay(offset, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, variant.get(offset));
            }
        } catch (Throwable t) {
            throw new AssertionError(
                "Replaying a compacted variant failed to load.\n"
                    + "  cleaned window=[" + from + ", " + to + ")\n"
                    + "  failed on surviving record " + offset + " " + variant.get(offset).key() + "\n"
                    + "  removedPositions=" + removedPositions(log, variant) + "\n"
                    + "  survivingRecords=" + variant.stream()
                        .map(record -> record.key().getClass().getSimpleName()
                            + (record.value() == null ? "(tombstone)" : ""))
                        .collect(Collectors.joining(", ")),
                t
            );
        }
    }

    private static List<Integer> removedPositions(List<CoordinatorRecord> log, List<CoordinatorRecord> variant) {
        List<Integer> removed = new ArrayList<>();
        int next = 0;
        for (int position = 0; position < log.size(); position++) {
            if (next < variant.size() && variant.get(next) == log.get(position)) {
                next++;
            } else {
                removed.add(position);
            }
        }
        return removed;
    }

    /**
     * The positions in {@code log} of the records log compaction is allowed to remove: a record
     * superseded by a later record with the same key, and any tombstone (which is itself removed once
     * {@code delete.retention.ms} expires). Every other record holds the latest value for its key and
     * must be retained.
     */
    private static Set<Integer> compactablePositions(List<CoordinatorRecord> log) {
        Set<ApiMessage> keysWrittenLater = new HashSet<>();
        Set<Integer> positions = new HashSet<>();
        for (int position = log.size() - 1; position >= 0; position--) {
            CoordinatorRecord record = log.get(position);
            if (record.value() == null || keysWrittenLater.contains(record.key())) {
                positions.add(position);
            }
            keysWrittenLater.add(record.key());
        }
        return positions;
    }

    /**
     * Removes every compactable record in {@code log[from..to)}, modelling a load that observes that
     * stretch of the log in its cleaned form.
     *
     * <p>A tombstone in the window is kept when an earlier record with the same key survives. Dropping
     * a tombstone needs {@code delete.retention.ms} to have expired, which is a later cleaner pass
     * than the one that collapsed the key, so a log in which a dropped tombstone is still preceded by
     * a record for the same key is not one a load can observe.
     */
    private static List<CoordinatorRecord> compact(
        List<CoordinatorRecord> log,
        Set<Integer> compactable,
        int from,
        int to
    ) {
        Set<ApiMessage> survivingKeys = new HashSet<>();
        List<CoordinatorRecord> compacted = new ArrayList<>(log.size());
        for (int position = 0; position < log.size(); position++) {
            CoordinatorRecord record = log.get(position);
            boolean cleaned = position >= from && position < to && compactable.contains(position);
            boolean isRetainedTombstone = record.value() == null && survivingKeys.contains(record.key());
            if (!cleaned || isRetainedTombstone) {
                compacted.add(record);
                survivingKeys.add(record.key());
            }
        }
        return compacted;
    }
}
