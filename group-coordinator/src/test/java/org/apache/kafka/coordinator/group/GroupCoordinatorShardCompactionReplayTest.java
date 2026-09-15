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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.CompactionReplayTestContext.ConsumerMemberState;
import org.apache.kafka.coordinator.group.CompactionReplayTestContext.StreamsMemberState;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.GroupLayout.Group;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.CompactionReplayTestContext.BAR_TOPIC_NAME;
import static org.apache.kafka.coordinator.group.CompactionReplayTestContext.FOO_TOPIC_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Compaction replay tests for the group coordinator. Tests capture written records,
 * compact the resulting log, and replay records through a new group coordinator
 * shard to verify loading.
 *
 * Two compaction cases are tested:
 *  - Prefix compaction (standard): A prefix of the log is compacted, so during loading,
 *    the group coordinator reads a compacted section followed by an uncompacted section.
 *  - Concurrent compaction: When compaction occurs concurrent with a load, the group
 *    coordinator can read a compacted section in between uncompacted sections.
 *    More precisely, something like the following can happen -
 *      1. Group coordinator loads segment A (uncompacted)
 *      2. Sections A and B are compacted
 *      3. Group coordinator loads sections B (compacted) then C (active, uncompacted)
 *    This scenario has been seen in production and caused KAFKA-19862.
 *
 * The compaction model in this test class aligns compaction to batch boundaries to match
 * realistic compaction performance. Consider a scenario like the following:
 *   1. ConsumerGroupMemberMetadataKey <- memberEpoch=0
 *   ...
 *   2. ConsumerGroupCurrentMemberAssignmentKey, compacted
 *   --- batch boundary ---
 *   3. ConsumerGroupCurrentMemberAssignmentKey = tombstone, compacted
 *   4. ConsumerGroupTargetAssignmentMemberKey = tombstone
 *   5. ConsumerGroupMemberMetadataKey = tombstone
 *
 * If we were to compact records 2,3 across a batch boundary, then record 5 will fail on load
 * because it will see memberEpoch=0 (from record 1) but expect LEAVE_GROUP_MEMBER_EPOCH. Put
 * another way, the batch boundaries mean that either all or no records for a given group
 * operation are compacted.
 */
public class GroupCoordinatorShardCompactionReplayTest {

    private static final GroupCoordinatorConfig REPLAY_CONFIG = GroupCoordinatorConfig.fromProps(Map.of());
    private static final GroupCoordinatorMetrics REPLAY_METRICS = new GroupCoordinatorMetrics();

    private Uuid fooTopicId;
    private Uuid barTopicId;
    private CoordinatorMetadataImage metadataImage;

    @BeforeEach
    public void setUp() {
        fooTopicId = Uuid.randomUuid();
        barTopicId = Uuid.randomUuid();
        metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, FOO_TOPIC_NAME, 6)
            .addTopic(barTopicId, BAR_TOPIC_NAME, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();
    }

    private CompactionReplayTestContext newContext() {
        MockPartitionAssignor consumerAssignor = new MockPartitionAssignor("range");
        MockTaskAssignor streamsAssignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.BIDIRECTIONAL.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(consumerAssignor))
            .withStreamsGroupTaskAssignors(List.of(streamsAssignor))
            .withMetadataImage(metadataImage)
            .build();
        return new CompactionReplayTestContext(context, consumerAssignor, streamsAssignor, metadataImage);
    }

    /**
     * Classic -> consumer group upgrade with offset commits.
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
    public void testClassicGroupUpgradeToConsumerGroup() throws Exception {
        CompactionReplayTestContext context = newContext();
        String groupId = "consumer-lifecycle-group";

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = context.joinFirstClassicMember(groupId);
        String classicMemberA = joinResponseA.memberId();
        context.syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
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
        ));

        // Offset commit
        context.commitOffset(groupId, FOO_TOPIC_NAME, 0, 10L);
        context.commitOffset(groupId, BAR_TOPIC_NAME, 0, 20L);

        // Member B joins with classic protocol, triggering rebalance
        String classicMemberB = context.joinClassicMember(groupId);

        // Member A rejoins
        JoinGroupResponseData rejoinResponseA = context.rejoinClassicMember(groupId, classicMemberA);
        context.syncClassicMember(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
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
        ));
        context.syncClassicMember(groupId, classicMemberB, rejoinResponseA.generationId(), Map.of());

        // Member C joins with consumer protocol, triggering an online classic -> consumer group upgrade.
        String memberC = Uuid.randomUuid().toString();
        context.prepareConsumerAssignment(Map.of(
            classicMemberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        Map<String, ConsumerMemberState> members = new LinkedHashMap<>();
        context.joinConsumerMember(groupId, memberC, members);

        // Members A and B move onto the consumer protocol one at a time.
        String memberA = Uuid.randomUuid().toString();
        context.prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        context.leaveClassicMember(groupId, classicMemberA);
        context.prepareConsumerAssignment(Map.of(
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2)),
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0))));
        context.waitForAssignmentInterval();
        context.joinConsumerMember(groupId, memberA, members);
        context.completeConsumerGroupRebalance(groupId, members);

        String memberB = Uuid.randomUuid().toString();
        context.prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2, 3), mkTopicAssignment(barTopicId, 0, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        context.leaveClassicMember(groupId, classicMemberB);
        context.completeConsumerGroupRebalance(groupId, members);
        context.prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1), mkTopicAssignment(barTopicId, 0)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 1)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5), mkTopicAssignment(barTopicId, 2))));
        context.waitForAssignmentInterval();
        context.joinConsumerMember(groupId, memberB, members);
        context.completeConsumerGroupRebalance(groupId, members);

        // Member D joins with consumer protocol, triggering a consumer group rebalance.
        String memberD = Uuid.randomUuid().toString();
        context.prepareConsumerAssignment(Map.of(
            memberA, mkAssignment(mkTopicAssignment(fooTopicId, 4, 5)),
            memberB, mkAssignment(mkTopicAssignment(fooTopicId, 0), mkTopicAssignment(barTopicId, 0)),
            memberC, mkAssignment(mkTopicAssignment(fooTopicId, 1), mkTopicAssignment(barTopicId, 1)),
            memberD, mkAssignment(mkTopicAssignment(fooTopicId, 2, 3), mkTopicAssignment(barTopicId, 2))));
        context.waitForAssignmentInterval();
        context.joinConsumerMember(groupId, memberD, members);
        context.completeConsumerGroupRebalance(groupId, members);

        // Group commits one more offset
        context.commitOffset(groupId, FOO_TOPIC_NAME, 1, 30L);

        // Verify the partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly(context);
    }

    /**
     * Classic -> streams upgrade with offset commits. Related bugs: KAFKA-20254
     *
     * Scenario:
     *  Classic group created
     *  Classic offset commit
     *  Classic group rebalance
     *  Group upgrades offline to streams protocol
     *  Members join/leave and group rebalances accordingly
     */
    @Test
    public void testClassicGroupMigratedToStreamsGroup() throws Exception {
        CompactionReplayTestContext context = newContext();
        String groupId = "streams-lifecycle-group";

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = context.joinFirstClassicMember(groupId);
        String classicMemberA = joinResponseA.memberId();
        context.syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ));

        // Offset commit
        context.commitOffset(groupId, FOO_TOPIC_NAME, 0, 10L);
        context.commitOffset(groupId, FOO_TOPIC_NAME, 1, 20L);

        // Member B joins with classic protocol, triggering rebalance
        String classicMemberB = context.joinClassicMember(groupId);

        // Member A rejoins
        JoinGroupResponseData rejoinResponseA = context.rejoinClassicMember(groupId, classicMemberA);
        context.syncClassicMember(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2)),
            classicMemberB, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ));
        context.syncClassicMember(groupId, classicMemberB, rejoinResponseA.generationId(), Map.of());

        // Group is shut down for offline upgrade to streams
        context.leaveClassicMember(groupId, classicMemberA);
        context.leaveClassicMember(groupId, classicMemberB);

        // Group restarts with streams protocol. The leftover classic group is tombstoned.
        String streamsMemberA = Uuid.randomUuid().toString();
        context.prepareStreamsAssignment(Map.of(streamsMemberA, context.tasks(0, 1, 2, 3, 4, 5)));
        Map<String, StreamsMemberState> members = new LinkedHashMap<>();
        context.joinStreamsMember(groupId, streamsMemberA, "process-a", members);
        context.completeStreamsGroupRebalance(groupId, members);

        // Member B joins and group rebalances.
        String streamsMemberB = Uuid.randomUuid().toString();
        context.prepareStreamsAssignment(Map.of(
            streamsMemberA, context.tasks(0, 1, 2),
            streamsMemberB, context.tasks(3, 4, 5)));
        context.waitForAssignmentInterval();
        context.joinStreamsMember(groupId, streamsMemberB, "process-b", members);
        context.completeStreamsGroupRebalance(groupId, members);

        // Member C joins and the group rebalances.
        String streamsMemberC = Uuid.randomUuid().toString();
        context.prepareStreamsAssignment(Map.of(
            streamsMemberA, context.tasks(0, 1),
            streamsMemberB, context.tasks(2, 3),
            streamsMemberC, context.tasks(4, 5)));
        context.waitForAssignmentInterval();
        context.joinStreamsMember(groupId, streamsMemberC, "process-c", members);
        context.completeStreamsGroupRebalance(groupId, members);

        // Member A leaves and the group rebalances.
        context.prepareStreamsAssignment(Map.of(
            streamsMemberB, context.tasks(0, 1, 2),
            streamsMemberC, context.tasks(3, 4, 5)));
        context.waitForAssignmentInterval();
        context.leaveStreamsMember(groupId, streamsMemberA, members);
        context.completeStreamsGroupRebalance(groupId, members);

        // Member B leaves and group rebalances (all tasks now owned by member C).
        context.prepareStreamsAssignment(Map.of(streamsMemberC, context.tasks(0, 1, 2, 3, 4, 5)));
        context.waitForAssignmentInterval();
        context.leaveStreamsMember(groupId, streamsMemberB, members);
        context.completeStreamsGroupRebalance(groupId, members);

        context.commitOffset(groupId, FOO_TOPIC_NAME, 2, 30L);

        // Verify partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly(context);
    }

    /**
     * Consumer -> classic downgrade by leave.
     *
     * Scenario:
     *  Classic group created and rebalanced
     *  Member joins with consumer protocol, upgrading the group to a consumer group
     *  The last consumer-protocol member leaves, downgrading the group back to classic
     *  Classic group commits an offset
     */
    @Test
    public void testConsumerGroupDowngradeByLeave() throws Exception {
        CompactionReplayTestContext context = newContext();
        String groupId = "consumer-downgrade-by-leave-group";

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = context.joinFirstClassicMember(groupId);
        String classicMemberA = joinResponseA.memberId();
        context.syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
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
        ));

        // Member B joins with classic protocol, triggering rebalance
        String classicMemberB = context.joinClassicMember(groupId);

        // Member A rejoins
        JoinGroupResponseData rejoinResponseA = context.rejoinClassicMember(groupId, classicMemberA);
        context.syncClassicMember(groupId, classicMemberA, rejoinResponseA.generationId(), Map.of(
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
        ));
        context.syncClassicMember(groupId, classicMemberB, rejoinResponseA.generationId(), Map.of());

        // Member C joins with the consumer protocol, upgrading the group online to a consumer group.
        // Members A and B stay on the classic protocol.
        String memberC = Uuid.randomUuid().toString();
        context.prepareConsumerAssignment(Map.of(
            classicMemberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2), mkTopicAssignment(barTopicId, 0)),
            classicMemberB, mkAssignment(mkTopicAssignment(fooTopicId, 3, 4, 5), mkTopicAssignment(barTopicId, 1, 2))));
        Map<String, ConsumerMemberState> members = new LinkedHashMap<>();
        context.joinConsumerMember(groupId, memberC, members);
        assertEquals(Group.GroupType.CONSUMER, context.groupType(groupId));

        // Member C, the last consumer-protocol member, leaves; the group downgrades back to classic
        // with members A and B.
        context.leaveConsumerMember(groupId, memberC, members);
        assertEquals(Group.GroupType.CLASSIC, context.groupType(groupId));

        // The classic group keeps working and commits an offset.
        context.commitOffset(groupId, FOO_TOPIC_NAME, 0, 40L);

        // Verify partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly(context);
    }

    /**
     * Consumer -> classic downgrade by static member replacement.
     *
     * Scenario:
     *  Classic group created
     *  Static member joins with consumer protocol, upgrading the group to a consumer group
     *  A classic member replaces the static consumer member, downgrading the group back to classic
     *  Classic group commits an offset
     */
    @Test
    public void testConsumerGroupDowngradeByStaticMemberReplacement() throws Exception {
        CompactionReplayTestContext context = newContext();
        String groupId = "consumer-downgrade-by-replacement-group";

        // A classic group is created when its first member joins and syncs
        JoinGroupResponseData joinResponseA = context.joinFirstClassicMember(groupId);
        String classicMemberA = joinResponseA.memberId();
        context.syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
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
        ));

        // A static member joins with the consumer protocol, upgrading the group online to a consumer
        // group. Member A stays on the classic protocol.
        String instanceId = "static-instance";
        String staticMemberId = Uuid.randomUuid().toString();
        context.prepareConsumerAssignment(Map.of(
            classicMemberA, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2), mkTopicAssignment(barTopicId, 0)),
            staticMemberId, mkAssignment(mkTopicAssignment(fooTopicId, 3, 4, 5), mkTopicAssignment(barTopicId, 1, 2))));
        Map<String, ConsumerMemberState> members = new LinkedHashMap<>();
        context.joinStaticConsumerMember(groupId, staticMemberId, instanceId, members);
        assertEquals(Group.GroupType.CONSUMER, context.groupType(groupId));

        // A classic member with the same instance id replaces the static consumer member. As it is the
        // last consumer-protocol member, the group downgrades back to classic.
        context.replaceStaticMemberWithClassicProtocol(groupId, instanceId);
        assertEquals(Group.GroupType.CLASSIC, context.groupType(groupId));

        // The classic group keeps working and commits an offset.
        context.commitOffset(groupId, FOO_TOPIC_NAME, 0, 50L);

        // Verify partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly(context);
    }

    /**
     * Streams -> classic offline downgrade. Related bugs: KAFKA-19862, KAFKA-20254
     *
     * Scenario:
     *  Streams group created and rebalanced
     *  Streams offset commit
     *  Group is shut down (all members leave), leaving an empty streams group
     *  Group restarts with the classic protocol, tombstoning the streams group
     */
    @Test
    public void testStreamsGroupOfflineDowngradeToClassicGroup() throws Exception {
        CompactionReplayTestContext context = newContext();
        String groupId = "streams-downgrade-group";

        // A streams group is created and members join and rebalance.
        String streamsMemberA = Uuid.randomUuid().toString();
        context.prepareStreamsAssignment(Map.of(streamsMemberA, context.tasks(0, 1, 2, 3, 4, 5)));
        Map<String, StreamsMemberState> members = new LinkedHashMap<>();
        context.joinStreamsMember(groupId, streamsMemberA, "process-a", members);
        context.completeStreamsGroupRebalance(groupId, members);
        assertEquals(Group.GroupType.STREAMS, context.groupType(groupId));

        // Offset commit
        context.commitOffset(groupId, FOO_TOPIC_NAME, 0, 10L);

        // Member B joins and the group rebalances.
        String streamsMemberB = Uuid.randomUuid().toString();
        context.prepareStreamsAssignment(Map.of(
            streamsMemberA, context.tasks(0, 1, 2),
            streamsMemberB, context.tasks(3, 4, 5)));
        context.waitForAssignmentInterval();
        context.joinStreamsMember(groupId, streamsMemberB, "process-b", members);
        context.completeStreamsGroupRebalance(groupId, members);

        // The group is shut down for offline downgrade to classic. Both members leave, one at a time,
        // leaving an empty streams group.
        context.prepareStreamsAssignment(Map.of(streamsMemberB, context.tasks(0, 1, 2, 3, 4, 5)));
        context.waitForAssignmentInterval();
        context.leaveStreamsMember(groupId, streamsMemberA, members);
        context.completeStreamsGroupRebalance(groupId, members);
        context.leaveStreamsMember(groupId, streamsMemberB, members);

        // Group restarts with the classic protocol. The leftover streams group is tombstoned.
        JoinGroupResponseData joinResponseA = context.joinFirstClassicMember(groupId);
        assertEquals(Group.GroupType.CLASSIC, context.groupType(groupId));
        String classicMemberA = joinResponseA.memberId();
        context.syncClassicMember(groupId, classicMemberA, joinResponseA.generationId(), Map.of(
            classicMemberA, List.of(
                new TopicPartition(FOO_TOPIC_NAME, 0),
                new TopicPartition(FOO_TOPIC_NAME, 1),
                new TopicPartition(FOO_TOPIC_NAME, 2),
                new TopicPartition(FOO_TOPIC_NAME, 3),
                new TopicPartition(FOO_TOPIC_NAME, 4),
                new TopicPartition(FOO_TOPIC_NAME, 5))
        ));
        context.commitOffset(groupId, FOO_TOPIC_NAME, 1, 20L);

        // Verify partitions can be reloaded cleanly from log.
        assertCompactedVariantsLoadCleanly(context);
    }

    /**
     * Replays every compacted variant of the captured log through a fresh group coordinator
     * and asserts that each one loads without throwing. Each variant cleans a single contiguous
     * window of record batches, modelling the three ways a load can observe compaction:
     * <ul>
     *   <li>uncompacted: the load reads the log exactly as written;</li>
     *   <li>compacted prefix: a prefix of the log is compacted, so the load reads a compacted section
     *       followed by the uncompacted tail. In this case, tombstones can be deleted to model
     *       {@code delete.retention.ms} elapsing (related to KAFKA-20254);</li>
     *   <li>concurrent compaction: a section in the middle of the log is compacted, so the coordinator
     *       reads an uncompacted section, then a compacted section, then the uncompacted tail (KAFKA-19862).
     *       Tombstones are retained as concurrent compaction is necessarily recent, so 
     *       {@code delete.retention.ms} is assumed to not have elapsed.</li>
     * </ul>
     */
    private void assertCompactedVariantsLoadCleanly(CompactionReplayTestContext context) {
        List<CoordinatorRecord> log = context.records();

        Set<Integer> compactableWithTombstoneDeletion = compactablePositions(log, true);
        Set<Integer> compactableWithoutTombstoneDeletion = compactablePositions(log, false);

        List<Integer> boundaries = context.batchBoundaries();

        // Uncompacted log
        assertLoadsCleanly(log, compactedPositions(log, compactableWithTombstoneDeletion, 0, 0));

        // Compacted prefix
        for (int lastBatch = 1; lastBatch < boundaries.size(); lastBatch++) {
            assertLoadsCleanly(log,
                compactedPositions(log, compactableWithTombstoneDeletion, 0, boundaries.get(lastBatch)));
        }

        // Concurrent compaction: the window starts partway through the log, leaving an uncompacted
        // section before it.
        for (int firstBatch = 1; firstBatch < boundaries.size() - 1; firstBatch++) {
            for (int lastBatch = firstBatch + 1; lastBatch < boundaries.size(); lastBatch++) {
                assertLoadsCleanly(log, compactedPositions(
                    log, compactableWithoutTombstoneDeletion, boundaries.get(firstBatch), boundaries.get(lastBatch)));
            }
        }
    }

    /**
     * The positions in {@code log} eligible for compaction: a record superseded by a later record
     * with the same key is always compactable. When {@code deleteTombstones} is set, a tombstone with
     * no later record for its key is also compactable, modelling {@code delete.retention.ms} elapsing.
     */
    private static Set<Integer> compactablePositions(List<CoordinatorRecord> log, boolean deleteTombstones) {
        Set<ApiMessage> laterKeys = new HashSet<>();
        Set<Integer> compactable = new HashSet<>();
        for (int position = log.size() - 1; position >= 0; position--) {
            CoordinatorRecord record = log.get(position);
            if (laterKeys.contains(record.key()) || (deleteTombstones && record.value() == null)) {
                compactable.add(position);
            }
            laterKeys.add(record.key());
        }
        return compactable;
    }

    /**
     * Replays {@code log} with {@code compactedPositions} removed through a real {@link
     * GroupCoordinatorShard} over a fresh coordinator, asserting the surviving records load without
     * throwing.
     */
    private void assertLoadsCleanly(List<CoordinatorRecord> log, Set<Integer> compactedPositions) {
        GroupMetadataManagerTestContext replayContext =
            new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.BIDIRECTIONAL.toString())
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

        int index = 0;
        int position = 0;
        try {
            for (; position < log.size(); position++) {
                if (compactedPositions.contains(position)) {
                    continue;
                }
                shard.replay(index, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, log.get(position));
                index++;
            }
        } catch (Throwable t) {
            throw new AssertionError(formatReplayFailure(log, compactedPositions, position), t);
        }
    }

    /**
     * Renders the whole log for a failed replay, one record per line with its position, marking
     * tombstones, records removed by compaction, and the record whose replay failed. For example:
     * <pre>
     *   0 | GroupMetadataKey(group='streams-lifecycle-group')
     *   ...
     *  21 | ConsumerGroupCurrentMemberAssignmentKey(groupId=..., memberId=...) = tombstone [compacted]
     *  22 | ConsumerGroupTargetAssignmentMemberKey(groupId=..., memberId=...) = tombstone
     *  23 | ConsumerGroupMemberMetadataKey(groupId=..., memberId=...) = tombstone &lt;-- replay failed
     * </pre>
     */
    private static String formatReplayFailure(
        List<CoordinatorRecord> log,
        Set<Integer> compactedPositions,
        int failedPosition
    ) {
        StringBuilder message = new StringBuilder("Replaying the log failed to load.\n");
        for (int position = 0; position < log.size(); position++) {
            CoordinatorRecord record = log.get(position);
            message.append(String.format("%3d | %s", position, record.key()));
            if (record.value() == null) {
                message.append(" = tombstone");
            }
            if (compactedPositions.contains(position)) {
                message.append(" [compacted]");
            }
            if (position == failedPosition) {
                message.append(" <-- replay failed");
            }
            message.append("\n");
        }
        return message.toString();
    }

    /**
     * The positions removed by cleaning the compactable records. A tombstone is
     * retained if an earlier surviving record shares its key, since the tombstone is still needed to
     * delete that record on load.
     */
    private static Set<Integer> compactedPositions(
        List<CoordinatorRecord> log,
        Set<Integer> compactable,
        int from,
        int to
    ) {
        Set<ApiMessage> survivingKeys = new HashSet<>();
        Set<Integer> removed = new HashSet<>();
        for (int position = 0; position < log.size(); position++) {
            CoordinatorRecord record = log.get(position);
            boolean cleaned = position >= from && position < to && compactable.contains(position);
            boolean isRetainedTombstone = record.value() == null && survivingKeys.contains(record.key());
            if (!cleaned || isRetainedTombstone) {
                survivingKeys.add(record.key());
            } else {
                removed.add(position);
            }
        }
        return removed;
    }
}
